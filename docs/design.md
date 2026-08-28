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
| `interface Collector<T,A,R>` | `Collector[T,A,R]` struct（函数字段） | struct 组合比接口多态更符合 Go 习惯 |
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

## 7. 并行预留（TODO）

当前串行。以下接口为 `Parallel(n)` 预留且已稳定：

- `Splitterator.TrySplit`：slice/range 已实现均衡二分
- 特征位：`SpOrdered` 决定合并是否需保序；`SpSubSized` 保证子源大小精确
- `Collector.Combiner`：全部预置收集器已实现分片合并语义
- `newStateful` 的 `limit+process` 签名：并行物化的扩展点

规划：TrySplit 递归分片至 n 份 → goroutine 各跑管道 → Combiner 按分片序合并（Ordered）→ 短路终止竞速。
