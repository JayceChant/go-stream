# Go Stream 库（Java Stream API 的 Go 1.27 泛型实现）Spec

> 版本：v2（按用户反馈修订：模块路径 / 并行延后 / API 详案展开 / 错误即值 / 文档要求 / 组合替代继承）

## Why

这是一个全新空白仓库（模块路径 `github.com/JayceChant/go-stream`，根包 `stream`）。目标：基于 Go 1.27 新落地的**泛型方法**特性，实现一套类似 Java Stream API 的流式处理库。Go 1.27 之前泛型方法不可用，`Stream[T]` 上的 `Map[U]` 这类"方法自带类型参数"的链式 API 无法实现，只能用包级函数（如 `stream.Map(s, f)`），链式手感全无；泛型方法落地后可以第一次用原生 Go 写出 `stream.Of(xs...).Filter(p).Map(f).ToSlice()` 的自然链式调用。

## 前置调研结论（Java Stream 实现原理 → Go 映射）

### Java Stream 核心机制（OpenJDK java.util.stream）

| Java 概念 | 职责 |
|---|---|
| `BaseStream`/`Stream<T>` | 公开 API；不存储数据，是惰性管道声明 |
| `AbstractPipeline` | 链表式 stage（source → op1 → op2 → terminal），`linkedOrConsumed` 防止重复消费 |
| `StatelessOp` | 仅在求值时把下游 Sink 包装成本 stage 的 Sink（`opWrapSink`），不触发遍历，单遍融合 |
| `StatefulOp` | 求值时需先驱动上游段产出临时容器（Node），再对新容器遍历执行下游段（分段求值） |
| `Sink<T>` | 推送式消费者链：`begin(size)`/`accept(t)`/`end()`/`cancellationRequested()` |
| `Spliterator<T>` | 数据源抽象：`tryAdvance`/`forEachRemaining`/`trySplit`/`characteristics` |
| `TerminalOp` | 封装终止操作的求值逻辑（collect/reduce/forEach/find/match...） |
| `Collector<T,A,R>` | 可组合汇聚：supplier/accumulator/combiner/finisher + characteristics |

求值流程：terminal 触发 `evaluate` → 从终端 sink 反向 `wrapSink` 逐级包装 → `copyInto` 用源 spliterator 推动数据单遍流过全部无状态 stage；短路（limit/findFirst/anyMatch）通过 `cancellationRequested` 提前终止；有状态 stage 处将管道切段，先物化再续段。

### Go 1.27 泛型方法的关键约束（决定架构）

1. 方法可以声明自己的类型参数：`func (s *Stream[T]) Map[U any](f func(T) U) *Stream[U]` ✅
2. **接口方法不能声明类型参数**，泛型方法也不能实现接口方法 ❌
   - ⇒ `Stream` 必须是**具体泛型 struct**，不能像 Java 那样以接口形态公开 API。
   - ⇒ 内部异构 stage 链（不同 E_IN/E_OUT）不能通过"接口 + 泛型方法"表达，采用**函数组合**（wrapSink 闭包）+ 上游引用链接，而非 Java 的类继承链。

### 针对 Go 的优化取舍

| 方面 | Java 做法 | Go 决策 |
|---|---|---|
| API 形态 | 接口 `Stream<T>` | 具体 struct `*Stream[T]` + 泛型方法（Go 1.27） |
| 元素推送 | `Consumer<T>`/`Sink` | `Sink[T]` 接口（只用 T，合法）+ `Accept` 返回 bool 融合 cancellationRequested |
| 数据源 | Spliterator（数组/Collection/IO/生成器） | `Splitterator[T]` 接口 + Go 原生源：slice、map、channel、`iter.Seq[T]`、生成器函数 |
| 原始类型特化 | IntStream/LongStream/DoubleStream 避免装箱 | **不需要**：Go 泛型值类型天然零装箱，用 `cmp.Ordered`/`Number` 约束即可 |
| 并行 | ForkJoinPool + trySplit | **阶段 1 不实现**，列入 TODO（见"阶段划分"）；接口层预留 TrySplit/特征位/Combiner |
| `Distinct` 需要 comparable | equals/hashCode | 方法不能追加约束：提供 `DistinctBy(key func(T) any)`（键须可比较）方法 + 包级 `Distinct[T comparable]` 双形态（Go 的 comparable 仅可作约束不能作普通类型） |
| 比较器 | `Comparator<T>`（int 返回） | 对齐 Go 1.21 `slices.SortFunc` 惯例：`func(a, b T) int`（`cmp.Compare` 风格） |
| 错误处理 | unchecked 异常穿透 | **错误即值**（详案见下）：可预期错误走 error 值；不可恢复错误 panic（详案见下） |
| 关闭资源 | onClose/BaseStream.close | 非目标（v1 不做）；channel 源自然耗尽 |
| 有状态算子物化 | Node.Builder 树 | 直接物化为 `[]T`（无并行时无需 Node 树） |

## 架构设计：组合替代继承（Java 抽象类 → Go 结构体）

Java Stream 的骨架是一棵**单继承类树**（`BaseStream` ← `AbstractPipeline` ← `ReferencePipeline` ← `Head`/`StatelessOp`/`StatefulOp`，配合 `ChainedReference`/`AbstractSpliterator` 等抽象基类，用"模板方法"让子类覆写 `opWrapSink`/`opEvaluateParallel`）。Go 不支持继承，本库按**组合优先**原则转换：**用"结构体嵌入 + 函数值字段 + 构造函数"取代"抽象类 + 子类覆写"**。

### 映射表

| Java 元素（抽象类/接口） | Go 等价物 | 转换手法 |
|---|---|---|
| `interface BaseStream<T,S>`（自类型递归 `S extends BaseStream<T,S>`） | 删除 | 单一具体类型 `*Stream[T]`，无需自类型泛型递归 |
| `abstract class AbstractPipeline` | 非导出 `pipeline[T]` struct（`drive` 求值闭包/source/chars/consumed/err 错误槽；**drive 在构造期捕获上游引用与 wrap 闭包**——Go 无 raw type，异构元素类型的上游无法存入同型字段，闭包组合是链表 stage 的类型安全等价物） | **嵌入组合**：`type Stream[T any] struct { pipeline[T] }`，公开方法定义在 `Stream` 上 |
| `ReferencePipeline.Head` | 构造函数 `newHead(src Splitterator[T]) *Stream[T]` | 构造函数取代子类型 |
| `abstract StatelessOp`（子类覆写 `opWrapSink`） | `newStateless(up *Stream[T], wrap func(down Sink[T]) Sink[T]) *Stream[T]` | **函数值取代模板方法**：`wrap` 闭包即 `opWrapSink` 的等价物；每个算子是"构造函数调用"，不是新类型 |
| `abstract StatefulOp`（`opEvaluateParallel`/分段物化） | `newStateful(up *Stream[T], limit int64, process func(buf []T) []T, chars)`：第一段经 collectingSink 物化（limit 可截断），第二段 process 变换后单遍回放 | 物化策略由闭包注入；「limit+process 两点式」取代最初的双闭包设计（续段 Begin/End/短路协议由引擎统一处理），同时是后续并行扩展点 |
| `interface Sink<T>` + `abstract ChainedReference<T,E_OUT>`（protected downstream 字段） | `Sink[T]` 接口 + 包内闭包 sink 适配器（捕获下游 sink 的函数/匿名 struct） | Go 无 protected 字段；下游 sink 由闭包捕获，`cancellationRequested` 融合为 `Accept` 的 bool 返回值 |
| `abstract PipelineHelper<P_OUT>` | 删除独立类型 | 其 `wrapSink`/`copyInto` 能力直接作为 `pipeline[T]` 的方法存在，无需类层次即可复用 |
| `TerminalOp` 实现类族（`ReduceOp`/`ForEachOp`/`FindOp`/`MatchOp`...） | 删除接口与类族 | 终止操作实现为 `*Stream[T]` 的导出方法，内部直接构造终止 sink；无用户侧多态扩展需求，**避免过度抽象**（简化点） |
| `interface Collector<T,A,R>` | `Collector[T,A,R]` **struct**（Supplier/Accumulator/Combiner/Finisher 函数字段 + 特征位） | 无多态必要；struct 字段组合更符合 Go 习惯，便于函数式装配 |
| `Spliterators.AbstractSpliterator`（trySplit 缓冲模板） | 非导出 `baseSplitterator[T]` struct（estSize/characteristics 公共字段） | **按需嵌入**：各源实现（slice/seq/channel/range/func）嵌入它获得公共字段与默认 TrySplit（返回 nil） |

### 嵌入使用原则

- `Stream[T]` 嵌入 `pipeline[T]`（核心求值机）——公开 API 与内部引擎分离。
- 每个 Splitterator 实现嵌入 `baseSplitterator[T]`——公共字段复用，仅覆写差异方法。
- 错误状态 `errState` 作为小 struct 嵌入 `pipeline[T]`，供错误即值模型使用。
- **不引入任何"抽象类风格"的半成品基类型**：Go 中"待定制行为"一律用函数值参数注入，不用"嵌入 + 期望覆写方法"模拟继承。

## 错误处理设计（Go error-as-value 详案）

原则（按用户要求）：**可预料且可纠正的错误按 Go 错误即值风格传播；难以预料且不可恢复的错误才 panic。**

### 分类决策

| 类别 | 例子 | 处理方式 |
|---|---|---|
| 可预期、可恢复 | 拉式源失败（IO/解析）、`MapErr` 回调返回错误 | **error 值传播**：求值短路终止，`Err()` 返回错误 |
| 编程 bug、不可恢复 | 重复消费已消费的流、传入 nil 回调 | **panic**（类似标准库对 nil 参数的行为），信息明确 |
| 用户回调自身 panic | 任意算子回调内部 panic | **原样传播**（不吞不包装语义，可附加求值阶段信息） |
| 语义策略问题 | `ToMap` 键冲突 | 非错误：**last-wins**（对齐 Go map 赋值惯例），文档明示；需要自定义时提供 `ToMapMerge(merge func(old, new V) V)` |

### 机制（Scanner 模式，Go 官方迭代器错误惯例）

参照 `bufio.Scanner.Err()` 与 `database/sql.Rows.Err()` 的官方模式：

1. **源侧**：`FromFunc(next func() (T, bool, error)) *Stream[T]`——拉式源（`next` 返回 `(元素, 是否还有, 错误)`），适配 IO/解析场景；错误记录、遍历停止。
2. **算子侧**：提供 Err 变体（仅四个高频算子，避免 API 膨胀）：`MapErr[U any](f func(T) (U, error)) *Stream[U]`、`FilterErr(p func(T) (bool, error)) *Stream[T]`、`FlatMapErr[U any](f func(T) ([]U, error)) *Stream[U]`、`PeekErr(f func(T) error) *Stream[T]`。首错发生：当前 stage 记录错误并令 `Accept` 返回 false（与短路机制同路），下游 `End()` 正常收尾，Collector/累积结果保持一致。
3. **终端侧**：`Err() error`——任意终止操作之后调用，返回求值过程中首个错误；无错误返回 nil。出错时终止操作返回**已累积的部分结果**（与 Scanner 一致，文档明示）。
4. **错误存储**：求值时创建共享错误槽，求值结束写回"发起终止调用的那个 Stream 实例"，`Err()` 从该实例读取。

### 明确不做的（保持简单路径简单）

- 普通算子（`Map`/`Filter`/...）回调**不**带 error 返回值：纯变换场景零错误噪声（对齐 `slices` 包风格）。
- Collector 的 Accumulator/Finisher 不引入错误签名：保持可组合性；fallible 汇聚用"先 `Collect` 到中间容器再校验"表达。

## API 设计详案（三层清单 + 建议）

### Tier A：必做（Java Stream 对齐）

**源（构造函数，全部惰性）**：`Of`/`FromSlice`/`FromSeq(iter.Seq)`/`FromChannel`/`FromMap[K,V] → *Stream[KV[K,V]]`/`FromFunc(next func() (T, bool, error))`/`Generate`/`Iterate`/`Range[I Integer]`/`Concat`/`Empty`

**无状态中间**：`Filter`/`Map[U]`/`FlatMap[U]`/`FlatMapSeq[U]`/`Peek`/`TakeWhile`/`DropWhile`

**有状态中间**：`Limit(n)`/`Skip(n)`/`Sorted(cmp func(a,b T) int)`（稳定排序，对齐 `slices.SortFunc`）/`DistinctBy(key)`/`Reverse`

**终止**：`ForEach`/`ForEachUntil(f func(T) bool)`/`ToSlice`/`Count`/`Reduce(identity, op)`/`ReduceOpt(op) (T, bool)`/`Collect[A,R]`/`First`/`FindAny`（顺序下同 First）/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err()`

**Collector 族**：`ToSlice`/`ToSet`/`ToMap`/`ToMapMerge`/`GroupingBy`/`Joining`/`Counting`/`Reducing`/`Mapping`

**Splitterator**：`TryAdvance(f func(T) bool) bool`/`ForEachRemaining`/`TrySplit()`/`EstimateSize()`/`Characteristics()`；特征位常量 SpSized/SpOrdered/SpSubSized/SpSorted/SpDistinct（Sp 前缀避免与 `Distinct` 函数等包级标识符冲突）；实现：slice（可二分）、range（可二分）、seq、channel、func 源（后三者不可分）

### Tier B：推荐新增（Go 风格 / 泛型方法 showcase）——**建议全部纳入**

| API | 形态 | 建议 | 理由 |
|---|---|---|---|
| `Enumerate[T any](s) *Stream[KV[int, T]]` | 包级函数 | ✅ 纳入 | 对应 Go `for i, v := range` 习惯；**实现时发现**：泛型方法返回 T 的派生类型（`Stream[KV[int,T]]`）触发 Go 1.27 实例化循环（T→KV[int,T]→KV[int,KV[int,T]]…），只能包级 |
| `Scan[U any](seed U, f func(U, T) U) *Stream[U]` | 方法 | ✅ 纳入 | 滚动累积/前缀和（含初值共 n+1 项）；**有状态但单遍无需物化**，展示引擎"有状态不分段"能力 |
| `Chunk[T any](s, n int) *Stream[[]T]` | 包级函数 | ✅ 纳入 | 批处理（批量写库/分页）高频需求；同 Enumerate 受实例化循环限制须包级 |
| `Zip[U, R any](o *Stream[U], f func(T, U) R) *Stream[R]` | 泛型方法 | ✅ 纳入 | 双流拉链；双类型参数方法是 Go 1.27 泛型方法的最佳 showcase |
| `FromFunc(next func() (T, bool, error))` | 构造 | ✅ 纳入 | 拉式 IO 源 + 错误即值入口（错误模型闭环） |
| `MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr` | 方法 | ✅ 纳入 | 错误即值核心（见错误处理设计） |
| 包级 `Contains[T comparable](*Stream[T], T) bool` | 包级函数 | ✅ 纳入 | 方法无法约束 `T comparable`，包级补偿 |
| 包级 `Sorted/Min/Max[T cmp.Ordered]` | 包级函数 | ✅ 纳入 | 同上，免写比较器的便捷形态 |
| 包级 `Sum/Avg[T Number](*Stream[T]) T` | 包级函数 | ✅ 纳入 | 数值聚合高频；`Number` 约束 = `Integer | Float` |
| 包级 `Distinct[T comparable]` | 包级函数 | ✅ 纳入 | 同 Contains 理由，与方法版 `DistinctBy` 互补 |

### Tier C：明确不做（v1，附理由）

- 原始特化流（IntStream 等）：Go 泛型零装箱，无需求
- `onClose`/资源管理流：channel 源自然耗尽；需要时后续加
- 可重放/可缓存流（memoize）：与一次性消费模型冲突
- Collector 错误化 Finisher：破坏组合简洁性
- `flatMapToInt` 特化族、流上 `iterator()` 双向遍历：无场景
- 限速/背压：channel 源天然具备，库层不掺和

### 建议摘要

Tier B 全部纳入的理由：`Scan`/`Zip`/`Chunk`/`Enumerate` 均为低成本高价值（复用既有引擎，无新机制）；包级函数族是 Go 泛型方法约束限制的**必要补偿**而非可选装饰；Err 族是错误模型闭环必需。Tier C 严守边界防止过度设计。

## 阶段划分（并行 = TODO）

- **阶段 1（本 spec 全部任务）**：串行核心引擎 + 全部 Tier A/B API + 错误即值模型 + Collector + 测试与基准 + Markdown 文档。
- ~~**后续 TODO**~~：`Parallel(n)`/`Sequential()` 并行求值——**已实现（Task 8，语义细化见「并行求值 v1」Requirement）**。
- **接口层从第一天为并行预留（不可省）**：`TrySplit`/`Characteristics`/`EstimateSize` 语义、Collector 的 `Combiner` 字段、`newStateful` 的物化闭包签名。

## What Changes

- **BREAKING**：无（全新仓库）
- 新建 Go module：`github.com/JayceChant/go-stream`，go 1.27，根包 `stream`
- 核心类型：`Stream[T]`（嵌入 `pipeline[T]`）、`Sink[T]`、`Splitterator[T]`（嵌入 `baseSplitterator[T]`）、`Collector[T,A,R]`、`KV[K,V]`、`Number`/复用 `cmp.Ordered` 约束
- 求值引擎：Sink 链反向包装、单遍融合、短路、有状态分段物化、一次性消费、错误即值短路
- API：Tier A + Tier B 全量；并行延后为 TODO（接口预留）
- 测试：单测 + `example_test.go`（可运行示例）+ 基准（vs 手写 for 循环）
- 文档（Markdown，任务化）：`README.md`、`docs/design.md`（架构与 Java 对照）、`docs/api.md`（API 参考）

## Impact

- Affected specs: 无（首个 spec）
- Affected code: 全部新增
  - `go.mod`、`stream.go`（Stream 类型/约束/KV）、`pipeline.go`（引擎+错误槽+consumed+newHead+evaluate）、`sink.go`、`spliterator.go`、`op.go`（newStateless/newStateful）、`sources.go`（各源 Splitterator 实现）、`construct.go`（包级构造函数）
  - `ops_stateless.go`（含 Err 变体）、`ops_stateful.go`（含 Scan/Chunk）、`op_ext.go`（Zip/Enumerate）
  - `terminal.go`（含 Err()）、`collector.go`、`numeric.go`（包级 Sum/Avg/Sorted/Min/Max/Contains/Distinct）
  - `*_test.go`、`example_test.go`、`benchmark_test.go`
  - `README.md`、`docs/design.md`、`docs/api.md`

## ADDED Requirements

### Requirement: Stream 构造（源适配）
系统 SHALL 提供包级构造函数，从多种容器/生成器类型构建 `*Stream[T]`，构造本身不触发任何遍历（惰性）：`Of`/`FromSlice`（零拷贝引用）/`FromSeq`/`FromChannel`/`FromMap`（产出 `KV[K,V]`，Unordered）/`FromFunc(next func() (T, bool, error))`（错误记录）/`Generate`（无限）/`Iterate`（无限）/`Range`（左闭右开）/`Concat`/`Empty`。

#### Scenario: 从 slice 构造并终止求值
- **WHEN** 用户执行 `FromSlice(s).Count()`
- **THEN** 返回 `len(s)`，且构造到求值前 `s` 未被遍历

#### Scenario: 无限源 + 短路
- **WHEN** 用户执行 `Generate(f).Limit(5).ToSlice()`
- **THEN** 正常终止并返回 5 个元素

#### Scenario: 拉式源错误
- **WHEN** `FromFunc(next)` 在第 3 次调用返回错误，用户 `ToSlice()` 后调 `Err()`
- **THEN** `ToSlice()` 返回前 2 个元素，`Err()` 返回该错误

### Requirement: 中间操作（惰性、返回新 Stream）
无状态（StatelessOp，单遍融合）：`Filter`/`Map[U]`/`FlatMap[U]`/`FlatMapSeq[U]`/`Peek`/`TakeWhile`（短路）/`DropWhile`；Err 变体：`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr`。
有状态（StatefulOp，物化上游段）：`Limit`（短路）/`Skip`/`Sorted`（稳定）/`DistinctBy`/`Reverse`；**单遍有状态**（不物化）：`Scan`；**包级单遍有状态**（实例化循环限制）：`Chunk`/`Enumerate`。
双流：`Zip[U, R]`（取短，两条流均被消费）。

#### Scenario: 无状态链单遍融合
- **WHEN** 对 N 元素源执行 `.Filter(p).Map(f).Count()`
- **THEN** 源只遍历一次，f 仅对通过 p 的元素调用

#### Scenario: Map 类型迁移
- **WHEN** 执行 `Of(1,2,3).Map(strconv.Itoa).ToSlice()`
- **THEN** 返回 `[]string{"1","2","3"}`，编译期静态类型检查

#### Scenario: 有状态操作分段
- **WHEN** 执行 `.Filter(p).Sorted(cmp).Map(f).ToSlice()`
- **THEN** 先物化排序（稳定）再单遍流过 map

#### Scenario: Err 变体短路
- **WHEN** `MapErr(f)` 中第 k 个元素转换出错
- **THEN** 源遍历立即停止，下游收到部分元素并正常 End()，`Err()` 返回该错误

### Requirement: 终止操作
`ForEach`/`ForEachUntil`/`ToSlice`/`Count`/`Reduce`/`ReduceOpt`/`Collect[A,R]`/`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err() error`；短路：`First`/`AnyMatch`/`AllMatch`/`NoneMatch`。

#### Scenario: AllMatch 短路
- **WHEN** 对 `[2,4,1,8]` 执行 `.AllMatch(even)`
- **THEN** 遇到 1 即返回 false，不遍历 8

### Requirement: Collector 汇聚抽象
`Collector[T,A,R]` struct（Supplier/Accumulator/Combiner/Finisher + 特征 IdentityFinish/Unordered），预置：`ToSlice`/`ToSet`/`ToMap`（last-wins）/`ToMapMerge`/`GroupingBy`（保遇序）/`Joining`/`Counting`/`Reducing`/`Mapping`。

#### Scenario: 分组保序
- **WHEN** `Of(p1,p2,...).Collect(GroupingBy(p.Id, p.Name))`
- **THEN** 返回 `map[ID][]string` 正确分组且组内保持遇序

### Requirement: Splitterator 抽象与特征位
接口五方法 + 特征位；slice/range 可二分 TrySplit（前后半段不重叠、并集完整）；seq/channel/func 不可分（返回 nil）；特征位沿管道传播规则（**修订**：Map/MapErr 为 1:1 变换，对齐 Java StreamOpFlag 只清 Sorted/Distinct、保留 Sized，使下游可按 size 预分配；Filter 保留全部；FlatMap 族 1:N 变换清 Sized/Sorted/Distinct；TakeWhile/DropWhile 清 Sized；Stateful 后段 SubSized...）。

### Requirement: 错误即值模型
可预期错误（FromFunc/Err 族）以 error 值传播：首错短路、部分结果保留、`Err()` 查询；不可恢复错误（重复消费、nil 回调）panic 且信息清晰；回调 panic 原样传播。

#### Scenario: 重复消费
- **WHEN** 对同一流两次 `ToSlice()`
- **THEN** 第二次 panic，提示流已被消费

### Requirement: 组合式架构
`Stream[T]` 嵌入 `pipeline[T]`；算子以构造函数 + wrap 闭包实现（无类继承层次）；Splitterator 实现嵌入 `baseSplitterator[T]`；库内不得出现"模拟抽象类待覆写"的基类型。

### Requirement: 包级便捷函数
`Contains[T comparable]`/`Sorted/Min/Max[T cmp.Ordered]`/`Sum/Avg[T Number]`/`Distinct[T comparable]`。

#### Scenario: 数值聚合
- **WHEN** `stream.Sum(stream.Range(0, 100))`
- **THEN** 返回 4950

### Requirement: 并行预留（本阶段不实现，TODO）
接口层 SHALL 保留 TrySplit/特征位/Combiner/物化闭包签名；README 路线图 SHALL 声明 `Parallel(n)` 为后续版本计划。

### Requirement: 并行求值 v1（Task 8 实现）
`Parallel(n)` 设置并行度、`Sequential()` 还原串行（均为中间操作语义：消费上游、返回携带标志的新流）。求值时满足以下条件才走并行路径，否则自动降级串行（正确性优先）：

- **分片机制**：pipeline 携带类型擦除的 `splitN` 闭包（沿链传播，可穿越 Map 等异构 stage——Go 无 raw type，无法以同型字段存源）；仅可分源（slice/range，即 TrySplit 非 nil 的源）在构造时设置。求值时递归 `TrySplit` 至 n 份（保序：前/后半段递归）。
- **分片求值**：每片 goroutine 独立重入 `p.drive`（head 层经 `ec.partSrc` 覆盖源），**每片全新 sink 链 + 独立终端累积**（避免共享 sink 的数据竞争）；物化分片结果后按分片序回放进用户终端（Ordered 保序；Unordered 收益留待 v2 流式合并）。
- **Collect 专属路径**：片级独立 `Supplier`+`Accumulator`，按分片序 `Combiner` 合并，`Finisher` 收尾。
- **降级规则**（splitN 置 nil 或 evaluateNP）：物化型有状态算子（Limit/Skip/Sorted/DistinctBy/Reverse）之后、单遍有状态（Scan/Chunk/Enumerate/DropWhile）、双流（Zip/Concat）、短路终止族（First/FindAny/AnyMatch/AllMatch/NoneMatch/ForEachUntil——保持串行短路优势）、不可分源——均串行。
- **错误与 panic**：片内首错按片序合并进主错误槽（部分结果保留）；片内回调 panic 捕获后由发起 goroutine 原样 re-panic。
- **验证**：`go test -race` 全绿；CPU 密集场景并行加速比 benchmark > 1.5x。

#### Scenario: 并行保序
- **WHEN** `FromSlice(0..9999).Parallel(4).Filter(p).Map(f).ToSlice()`
- **THEN** 结果与串行完全一致（分片序回放）

#### Scenario: 管道含状态算子自动降级
- **WHEN** `FromSlice(xs).Parallel(4).Sorted(cmp).ToSlice()`
- **THEN** 正确排序（串行求值），不 panic

### Requirement: 文档（Markdown）
SHALL 交付：`README.md`（简介/安装/快速上手/API 速览/与 Java 对照/设计要点/路线图含并行 TODO）、`docs/design.md`（架构原理：管道/Sink/Splitterator/分段求值/错误模型/组合替代继承映射表）、`docs/api.md`（分组 API 参考 + 示例）；`example_test.go` 提供可运行示例（与文档示例一致）。

### Requirement: 质量保障
全部公开 API 中文 godoc；`go vet`/`go test ./...` 全绿；benchmark：`Filter+Map+ToSlice` 相对手写 for 循环额外开销目标 <3x。

## 非目标（Non-Goals）
- 并行求值实现（阶段 1；接口预留）——后续 TODO 必做
- Collector 错误化 Finisher；onClose/资源管理；可重放流；原始特化流；限速/背压（见 Tier C）

## MODIFIED Requirements
无（全新项目）。

## REMOVED Requirements
无。
