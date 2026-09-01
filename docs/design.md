# 架构设计

> 本文阐述 go-stream 的核心实现原理：如何把 Java Stream 的抽象类层次转换为 Go 组合式结构，以及求值引擎、错误模型的关键机制。

## 1. 从 Java 类层次到 Go 组合

Java Stream 的骨架是一棵**单继承类树**（`BaseStream` ← `AbstractPipeline` ← `ReferencePipeline` ← `Head`/`StatelessOp`/`StatefulOp`），用模板方法让子类覆写 `opWrapSink` 等钩子。Go 不支持继承，本库按**组合优先**转换：用「结构体嵌入 + 构造函数 + 函数值注入」取代「抽象类 + 子类覆写」。

### 映射表

| Java 元素 | Go 等价物 | 转换手法 |
|---|---|---|
| `interface BaseStream<T,S>`（自类型递归） | 单一 `*Stream[T]` | 无需自类型泛型递归 |
| `abstract AbstractPipeline` | 非导出 `pipeline[T]` struct | **嵌入组合**：`type Stream[T] struct { pipeline[T] }` |
| `ReferencePipeline.Head` | 构造函数 `newHead(src)` | 构造函数取代子类型 |
| `abstract StatelessOp`（覆写 `opWrapSink`） | `newStateless(up, wrap)` | **函数值取代模板方法**：wrap 闭包即 opWrapSink 等价物 |
| `abstract StatefulOp`（分段物化） | `newStateful(up, limit, process)` | 物化策略由闭包注入（limit+process 两点式） |
| `interface Sink<T>` + `ChainedReference` | `Sink[T]` 接口 + 闭包 sink 适配器 | `cancellationRequested` 融合为 `Accept` 的 bool 返回 |
| `TerminalOp` 类族 | `*Stream[T]` 的导出方法 | 无用户侧多态扩展需求，避免过度抽象 |
| `interface Collector<T,A,R>` | `collector.Collector[T,A,R]` struct（函数字段，独立子包） | struct 组合比接口多态更符合 Go 习惯；收集器族与引擎零耦合，独立成 `stream/collector` 零依赖叶子包（`Summing` 因依赖 `Number` 约束留根包） |
| `abstract AbstractSpliterator` | `baseSplitterator[T]` struct | 按需嵌入复用公共字段与默认 TrySplit |

### 嵌入原则

- `Stream[T]` 嵌入 `pipeline[T]`：公开 API 与内部引擎分离
- 各源 Splitterator 嵌入 `baseSplitterator[T]`：公共字段复用
- **不引入任何「模拟抽象类待覆写」的基类型**：待定制行为一律以函数值参数注入

## 2. drive 闭包：类型安全的 stage 链

Go 无 raw type：`Map` 前后元素类型不同（`Stream[T]` → `Stream[U]`），异构上游无法存入同型 `upstream` 字段。本库以**构造期捕获闭包**替代链表 stage：

```go
func newStateless[T, U any](up *Stream[T], wrap ..., chars) *Stream[U] {
    ud := up.drive // 构造期捕获上游求值闭包
    return &Stream[U]{pipeline[U]{
        drive: func(down Sink[U], ec *evalCtx) {
            ud(wrap(down, ec), ec) // 求值时先包装下游再驱动上游
        },
        chars: chars,
    }}
}
```

每个 stage 的 `drive` 闭包封装「源 → … → 上游 → 本 stage」整段求值语义；链接即组合闭包，全程编译期类型安全——这是 Java 链表式 AbstractPipeline 的类型安全等价物。

## 3. 求值引擎

### 单遍融合（无状态算子）

终止操作触发 `evaluate`：创建共享 `evalCtx`，调用 drive 链。求值从终止 sink 出发**反向包装**：每级无状态算子的 wrap 把下游 sink 变成本级 sink，最终形成一条 Sink 链；然后由数据源（Splitterator）**单遍**推动元素流过整条链。全部无状态算子融合进这一遍，无中间容器。

```go
type Sink[T any] interface {
    Begin(size int64)        // 推送开始（size 为源估计大小，未知 -1）
    Accept(t T) bool         // 返回 false 请求取消（短路）
    End()                    // 推送结束（含短路路径）
}
```

#### 三个顺序：构造、包装与数据流

drive/wrap/down 的协作涉及三个**方向不一致**的顺序，是理解求值引擎的关键：

| 阶段 | 方向 | 发生时机 | 内容 |
|---|---|---|---|
| 链接（构造期） | 源 → 终端（正向） | `.Map().Filter()` 调用时 | 闭包洋葱式嵌套，**惰性**：wrap 仅被捕获、一次不执行；越后链接的 stage 其 drive 越靠外层（封装整段链） |
| 包装（求值期第一步） | 终端 → 源（**反向**） | 终端操作触发 evaluate 后 | 从终止 sink 出发逐级调用 wrap，把 down 包装成本级 sink，自外向内完成 Sink 链装配 |
| 数据流（求值期第二步） | 源 → 终端（正向） | Sink 链装配完成后 | Head 段驱动源单遍推元素，穿过整条 Sink 链抵达终端 |

以 `FromSlice(xs).Map(f).Filter(p).Collect(...)` 为例，求值时序：

```text
① 反向包装（wrap 调用序）：drive₃(term)
     Filter.wrap(term) → filterSink{down: term}
     → drive₂(filterSink)
         Map.wrap(filterSink) → mapSink{down: filterSink}
         → drive₁(mapSink)                     // 抵达 Head
② 正向推送（数据流序）：drive₁ 遍历源
     t → mapSink.Accept → f(t) → filterSink.Accept → p(u) → term.Accept
```

核心一行 `ud(wrap(down, ec), ec)`（见第 2 节）的读法：**先用 wrap 把 down 包装成本级 sink，再交给上游 drive 驱动**——wrap 必须先于上游执行（管道先建成，数据才能流过）；down 是 wrap 的入参而非出参（本级变换是"装饰下游"）；ud 是构造期捕获的上游闭包（递归回溯到源头才开始推数据）。

常见误读与纠正：

- **误**：`.Map(f)` 的变换逻辑应该在"处理上游输出"的代码里。**正**：它写在"包装下游"的装饰器里——`mapSink.Accept` 收到元素即刻变换并转交已持有的 down（装饰器视角，非数据视角）。
- **误**：drive 是"执行本 stage 一级"。**正**：drive 语义是"驱动从源到本 stage 的整段"，故越晚链接的 stage 拥有越大的 drive。
- **误**：构造链时数据已流动。**正**：链接仅组合闭包，全部元素流动发生在终端操作的 evaluate 内。

记忆口诀：**wrap 逆流而上建管道，数据顺流而下过管道**。

### 短路取消

`Accept` 返回 false 即短路：源立即停止遍历，`End()` 恒被调用（终端 sink 观察到配对完整的协议）。`Limit`/`First`/`AnyMatch`/`TakeWhile` 等由此实现无限流安全。

### 分段求值（有状态算子）

`Sorted`/`Skip`/`DistinctBy`/`Reverse` 等需全量信息，采用**两点式物化**：

1. **第一段**：驱动上游把元素收集进 `collectingSink`（`limit` 可截断，支持 `Limit` 对无限源的短路收集）
2. **第二段**：`process` 纯切片变换（排序/去重/跳过等）后单遍回放，续段 Begin/End/短路协议由引擎统一处理

```go
func newStateful[T any](up *Stream[T], limit int64, process func([]T) []T, chars) *Stream[T]
```

`Scan`/`Chunk`/`Enumerate` 为单遍有状态（滚动状态无需物化），不切段。

### 特征位传播

`SpSized`/`SpOrdered`/`SpSubSized`/`SpSorted`/`SpDistinct` 沿管道传播（对齐 Java StreamOpFlag）：

- `Filter`：保留全部（不改变结构性质）
- `Map`/`MapErr`（1:1）：保留 `SpSized`（下游按 size 预分配），清 `SpSorted`/`SpDistinct`
- `FlatMap` 族（1:N）：清 `SpSized`/`SpSorted`/`SpDistinct`
- `TakeWhile`/`DropWhile`：清 `SpSized`
- 物化后：置 `SpSized`+`SpSubSized`，`Sorted` 置 `SpSorted`

特征位当前用于 size 预分配优化，并为并行拆分（TrySplit 均衡性、有序合并）预留决策依据。

## 4. Splitterator：数据源抽象

```go
type Splitterator[T any] interface {
    TryAdvance(f func(T) bool) bool
    ForEachRemaining(f func(T) bool)
    TrySplit() Splitterator[T] // 返回后半段，自身收缩为前半段；不可分返回 nil
    EstimateSize() int64       // 未知 -1
    Characteristics() Characteristics
}
```

| 源 | 可分 | 说明 |
|---|---|---|
| slice | ✅ | 零拷贝区间二分（`newSliceSp`） |
| range | ✅ | 溢出安全中点二分 |
| seq（`iter.Seq`） | ❌ | `iter.Pull` 拉取式，支持取消释放 |
| channel | ❌ | 阻塞接收 |
| func（`FromFunc`） | ❌ | 拉式 IO/解析，错误即值入口 |

`TrySplit` 保证前后半段**不重叠且并集完整**、有序语义保持遇序——这是后续 `Parallel(n)` 并行分片的接口基础。

## 5. 错误即值模型

参照 `bufio.Scanner.Err()` / `sql.Rows.Err()` 官方惯例：

| 类别 | 例子 | 处理 |
|---|---|---|
| 可预期、可恢复 | `FromFunc` 源失败、`MapErr` 族回调错误 | error 值传播：首错短路、部分结果保留、`Err()` 查询 |
| 编程 bug | 重复消费、nil 回调 | panic（信息明确） |
| 用户回调 panic | 任意算子 | 原样传播 |
| 语义策略 | `ToMap` 键冲突 | last-wins（文档明示），`ToMapMerge` 自定义合并 |

机制：求值创建共享 `evalCtx`（mutex 保护首错槽，Zip 双流并发写安全）；Err 变体 sink 以 `return ec.fail(err)` 一句表达「记录首错 + 请求短路」；错误与短路同路，下游 `End()` 正常收尾，累积结果保持一致；求值结束首错写回发起终止调用的 Stream 实例，`Err()` 从该实例读取。

普通算子回调**不带** error 签名（纯路径零噪声，对齐 `slices` 包风格）。

## 6. 一次性消费

每个 Stream 实例仅可被链接（作为上游）或消费（终止求值）一次：`checkLinked` 检查并置位 `consumed`，重复使用 panic（`errConsumed`）——与 Java `linkedOrConsumed` 语义一致，防止流的隐式重放。

## 7. 并行求值（Parallel v1）

`Parallel(n)` 声明后续求值最多 n 个分片并行；`Sequential()` 还原串行；`Unordered()` 清除 SpOrdered（声明不依赖相遇顺序）。均为「纯标志 stage」：不改变元素流，仅改写 `pipeline.parN`/`chars`。

### 分片机制

Go 无 raw type，异构 stage 链（Map 前后元素类型不同）无法持有同型 source 字段。因此 Head 构造时设置**类型擦除的分片闭包** `splitN func(n int) []any`，沿链原样传播（可穿越 Map 异构边界）；Head 段求值闭包经 `ec.partSrc` 断言恢复具体类型（构造拓扑保证一致）。

求值期递归 `TrySplit` 至 n 份（保序二分：前半段 n/2 份 + 后半段 n-n/2 份）。仅可分源（slice/range）构造时设置 splitN。

### 分片求值与合并

每片 goroutine **独立重入** `p.drive`（全新 sink 链 + 独立终端累积，无共享可变状态）；片级终端由主 goroutine 在启动前串行预创建（登记安全）。完成后：

- **通用路径**（ToSlice/ForEach/Min/Max）：各片物化，按分片序回放进用户终端（Ordered 保序；回放中短路即广播取消）
- **Collect 专属**：片级独立 Supplier+Accumulator，按分片序 `Combiner` 合并
- **Count/Reduce**：片内聚合，total 求和/合并
- **无序流式合并**（`Unordered()` 或天然无序源如 `FromMap`，SpOrdered 缺失）：片完成即推——通用路径按元素级先完成先推（终端实现 `pushPart`，`down.Begin(-1)` 总量未知，终端取消则停止后续推送）；Collect 按片级 Combiner 完成序合并。结果集合与串行一致，顺序不保证（无序语义）；Count/Reduce 无增量推入语义，仍片序聚合

### 降级规则（正确性优先）

满足任一即自动串行，无需用户干预：

- 物化型有状态算子之后（`Limit`/`Skip`/`Sorted`/`DistinctBy`/`Reverse` 置 splitN=nil）
- 单遍有状态（`Scan`/`Chunk`/`Enumerate`/`DropWhile`）
- 双流算子（`Zip`/`Concat`）
- 短路终止族（`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`/`ForEachUntil`——保持串行短路优势）
- 不可分源（splitN 未设置或 TrySplit 返回 nil）

### 错误与 panic

片内首错（`FromFunc`/`MapErr` 族）按片序并入主错误槽；**出错片截断于错误点，其余片结果完整保留**（分片粒度的部分结果）。片内用户回调 panic 被捕获后由发起 goroutine 原样 re-panic。

### 实测

CPU 密集场景（200k 元素 × 200 次循环体）4 分片加速比 ~3.3x（`TestParallel_Speedup` 可复现）。

## 8. 生命周期与可重放（Task 10）

### OnClose/Close 回调链

`pipeline` 携带 `closers []func() error`（按注册序），中间操作沿链继承（`newStateless`/`newStateful`/`newFlagStage`），组合流经 `mergeClosers` 合并双方（Concat 按 a 先 b 后、Zip 按本流先 other 后——与求值序一致）。

- **自动触发**：`evaluateNP` 以 defer 调 `runClosers`，覆盖正常耗尽、短路、错误值与回调 panic 展开路径；回调错误并入错误槽首错保留
- **显式 Close**：幂等（`closed` 标志 + 每回调 `sync.Once` 双保险——多实例链/组合流触发也恰好一次）；未求值流可关闭，此后求值收尾不重复触发
- **并行求值**：片 goroutine 重入 `p.drive` 不触发（closers 在 pipeline 实例上，仅收尾路径调用一次）

### Cache 可重放工厂

`Cache(s) func() *Stream[T]`：`sync.Once` 保证首次调用求值 s 一次并物化；此后每次 `FromSlice(buf)` 返回全新一次性流（共享底层数组零拷贝）。物化期首错记忆进工厂，此后每次返回 `emptyWithErr` 携带错误的空流（任何终止操作得空结果、`Err()` 可查）——一次性模型全程不被破坏：s 被消费一次，产物各一次性。

### Unordered 流式合并

见第 7 节「无序流式合并」条目：`streamTotal` 扩展协议（`pushPart` 返回 bool 支持取消），`evaluateParallelStream` 以完成序通道驱动，`down.Begin(-1)`（总量未知）先行、全部片处理完 `down.End()` 收尾。
