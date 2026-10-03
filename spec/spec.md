# Go Stream 库（Java Stream API 的 Go 1.27 泛型实现）Spec

> 编写格式：Requirement（SHALL 条款）+ Scenario（WHEN/THEN，落到可核对的行为）+ 决策依据（关键取舍原因，防反复）；精简纪律（上限 400 行与 legacy 转移）见 [`agents/common.md`](../agents/common.md) 1.1 节。

## Why

基于 Go 1.27 **泛型方法**特性、对标 Java Stream API 的流式处理库（模块 `github.com/JayceChant/go-stream`，go 1.27，根包 `stream`）。泛型方法落地之前，`Stream[T]` 上的 `Map[U]` 这类"方法自带类型参数"的链式 API 无法实现，只能用包级函数（如 `stream.Map(s, f)`）；本库的目标即以原生 Go 提供自然链式调用：`stream.Of(xs...).Filter(p).Map(f).ToSlice()`。

## 前置调研结论（Java Stream 实现原理 → Go 映射）

### Java Stream 核心机制（OpenJDK java.util.stream）

| Java 概念 | 职责 |
|---|---|
| `AbstractPipeline` | 链表式 stage（source → op1 → op2 → terminal），`linkedOrConsumed` 防止重复消费 |
| `StatelessOp` | 仅在求值时把下游 Sink 包装成本 stage 的 Sink（`opWrapSink`），不触发遍历，单遍融合 |
| `StatefulOp` | 求值时需先驱动上游段产出临时容器（Node），再对新容器遍历执行下游段（分段求值） |
| `Sink<T>` | 推送式消费者链：`begin(size)`/`accept(t)`/`end()`/`cancellationRequested()` |
| `Spliterator<T>` | 数据源抽象：`tryAdvance`/`forEachRemaining`/`trySplit`/`characteristics` |
| `Collector<T,A,R>` | 可组合汇聚：supplier/accumulator/combiner/finisher + characteristics |

### Go 1.27 泛型方法的关键约束（决定架构）

以下能力与限制均已在 go1.27.0 实测验证：

1. ✅ 方法可以声明自己的类型参数：`func (s *Stream[T]) Map[U any](f func(T) U) *Stream[U]`
2. ✅ 方法**自有**类型参数可带 `any` 以外的约束（含 `comparable`）：如 `func (s *Stream[T]) Group[K comparable](key func(T) K) map[K][]T` 合法，调用处按实参推断 K。
   - 注意：`any` 满足 `comparable`（Go 1.20+ 接口类型满足该约束），故键函数显式返回 `any` 仍能编译——约束只排除**具体不可比较类型**的键（编译期报错），不能根除动态类型不可比较时的运行时 map panic。
3. ❌ **接口方法不能声明类型参数**，泛型方法也不能实现接口方法
   - ⇒ `Stream` 必须是**具体泛型 struct**，不能像 Java 那样以接口形态公开 API。
   - ⇒ 内部异构 stage 链（不同 E_IN/E_OUT）不能通过"接口 + 泛型方法"表达，采用**函数组合**（wrapSink 闭包）+ 上游引用链接，而非 Java 的类继承链。
4. ❌ **方法不能约束接收者已有的类型参数**：`func (s *Stream[T comparable]) …` 语法错误（missing ',' in type argument list）⇒ 需要约束 `T` 本身的 API（`Distinct`/`Contains`）在 `*Stream[T]` 上只能以包级函数提供；解法：以自有类型参数带约束的包装类型（`NumberStream[N Number]` 嵌入 `Stream[N]`）把约束收窄到类型参数位，此类 API 回归方法形态（见「NumberStream 数值流」Requirement）。
5. ❌ **方法返回 T 的派生类型触发实例化循环**：`func (s *Stream[T]) Chunk(n int) *Stream[[]T]` 报 instantiation cycle（T → []T → [][]T → …）⇒ `Chunk`/`Enumerate` 等只能以包级函数提供。

判定口诀：方法**新增**类型参数（任意约束）均可；一旦要**动 T 本身**——约束它、或让其以派生类型出现在返回值——就必须降级为包级函数（项目形态原则详见「形态原则与方法化迁移」Requirement）。

### 针对 Go 的优化取舍

| 方面 | Java 做法 | Go 决策 |
|---|---|---|
| API 形态 | 接口 `Stream<T>` | 具体 struct `*Stream[T]` + 泛型方法（Go 1.27） |
| 元素推送 | `Consumer<T>`/`Sink` | `Sink[T]` 接口（只用 T，合法）+ `Accept` 返回 bool 融合 cancellationRequested |
| 数据源 | Spliterator（数组/Collection/IO/生成器） | `Splitterator[T]` 接口 + Go 原生源：slice、map、channel、`iter.Seq[T]`、生成器函数 |
| 原始类型特化 | IntStream/LongStream 等避免装箱 | **不需要**：Go 泛型值类型天然零装箱；约束收窄价值由 `NumberStream` 承接 |
| 并行 | ForkJoinPool + trySplit | `Parallel(n)`/`Sequential()`：TrySplit 分片 + goroutine；短路终止与物化算子后自动降级串行（见「并行求值 v1」） |
| `Distinct` 需要 comparable | equals/hashCode | 双形态：方法 `DistinctBy[K comparable](key func(T) K)`（键类型编译期可比较、map[K] 零装箱）+ 包级 `Distinct[T comparable]`（方法不能约束接收者 T；Go 的 comparable 仅可作约束不能作普通类型） |
| 比较器 | `Comparator<T>`（int 返回） | 对齐 Go 1.21 `slices.SortFunc` 惯例：`func(a, b T) int`（`cmp.Compare` 风格） |
| 错误处理 | unchecked 异常穿透 | **错误即值**（详案见下）：可预期错误走 error 值；不可恢复错误 panic |
| 关闭资源 | onClose/BaseStream.close | `OnClose(f)`/`Close()`：求值结束自动触发 + 显式幂等释放 |
| 有状态算子物化 | Node.Builder 树 | 直接物化为 `[]T`（无并行时无需 Node 树） |

## 架构设计：组合替代继承（Java 抽象类 → Go 结构体）

Java Stream 的骨架是一棵**单继承类树**（用"模板方法"让子类覆写 `opWrapSink`/`opEvaluateParallel`）。Go 不支持继承，本库按**组合优先**原则转换：**用"结构体嵌入 + 函数值字段 + 构造函数"取代"抽象类 + 子类覆写"**。

### 映射表

| Java 元素 | Go 等价物 | 转换手法 |
|---|---|---|
| `interface BaseStream<T,S>`（自类型递归） | 删除 | 单一具体类型 `*Stream[T]`，无需自类型泛型递归；公开 API 为惰性管道声明 |
| `abstract class AbstractPipeline` | 非导出 `pipeline[T]` struct（`drive driveFunc[T]` 求值闭包/source/chars/consumed/err 错误槽） | **嵌入组合**：`type Stream[T any] struct { pipeline[T] }`；drive 在构造期捕获上游引用与 wrap 闭包——Go 无 raw type，异构元素类型的上游无法存入同型字段，闭包组合是链表 stage 的类型安全等价物；包裹方向由元素类型链锁死（调用方持有的返回值恒为最后一级 `*Stream[R]`，沿捕获链向内逐级回退至源类型），故「后级包前级」不可避免 |
| `ReferencePipeline.Head` / `abstract StatelessOp` | 构造函数 `newHead(src)` / `newStateless(up, wrap)` | 构造函数取代子类型；**函数值取代模板方法**——wrap 闭包即 opWrapSink 等价物，每个算子是"构造函数调用"，不是新类型 |
| `abstract StatefulOp`（opEvaluateParallel/分段物化） | `newStateful(up, limit, process, chars)` | 第一段经 collectingSink 物化（limit 可截断），第二段 process 变换后单遍回放；「limit+process 两点式」物化策略由闭包注入，同时是并行扩展点 |
| `interface Sink<T>` + `abstract ChainedReference` | `Sink[T]` 接口 + 包内闭包 sink 适配器 | Go 无 protected 字段；下游 sink 由闭包捕获，`cancellationRequested` 融合为 `Accept` 的 bool 返回值 |
| `abstract PipelineHelper<P_OUT>` / `TerminalOp` 实现类族 | 均删除 | wrapSink/copyInto 直接作为 `pipeline[T]` 的方法；终止操作为 `*Stream[T]` 导出方法（内部直接构造终止 sink）——**避免过度抽象**（简化点） |
| `interface Collector<T,A,R>` | `Collector[T,A,R]` **接口**（Supplier/Accumulator/Combiner/Finisher 四方法；各收集器为非导出具体类型实现，Combiner 返回 nil 表达不支持并行） | 接口形态保证行为只读（struct 函数字段可被外部改写）；自定义收集器实现同一边即可 |
| `Spliterators.AbstractSpliterator` | 非导出 `baseSplitterator[T]` struct | **按需嵌入**：各源实现（slice/seq/channel/range/func）嵌入它获得公共字段与默认 TrySplit（返回 nil） |

### 嵌入使用原则

`Stream[T]` 嵌入 `pipeline[T]`（公开 API 与内部引擎分离）；Splitterator 实现嵌入 `baseSplitterator[T]`；错误状态 `errState` 嵌入 `pipeline[T]` 供错误即值模型使用。**不引入任何"抽象类风格"的半成品基类型**："待定制行为"一律用函数值参数注入，不用"嵌入 + 期望覆写方法"模拟继承。

## 错误处理设计（Go error-as-value 详案）

原则：**可预料且可纠正的错误按 Go 错误即值风格传播；难以预料且不可恢复的错误才 panic。**

### 分类决策

| 类别 | 例子 | 处理方式 |
|---|---|---|
| 可预期、可恢复 | 拉式源失败（IO/解析）、`MapErr` 回调返回错误 | **error 值传播**：求值短路终止，`Err()` 返回错误 |
| 编程 bug、不可恢复 | 重复消费已消费的流、传入 nil 回调 | **panic**（类似标准库对 nil 参数的行为），信息明确 |
| 用户回调自身 panic | 任意算子回调内部 panic | **原样传播**（不吞不包装语义，可附加求值阶段信息） |

`ToMap` 键冲突为语义策略问题而非错误：**last-wins**（对齐 Go map 赋值惯例），文档明示；需要自定义时提供 `ToMapMerge(merge func(oldV, newV V) V)`。

### 机制（Scanner 模式，Go 官方迭代器错误惯例）

参照 `bufio.Scanner.Err()` 与 `database/sql.Rows.Err()` 的官方模式：

1. **源侧**：`FromFunc(next func() (T, bool, error)) *Stream[T]`——拉式源（`next` 返回 `(元素, 是否还有, 错误)`），适配 IO/解析场景；错误记录、遍历停止。
2. **算子侧**：提供 Err 变体（仅四个高频算子，避免 API 膨胀）：`MapErr[U any](f func(T) (U, error))`、`FilterErr(p func(T) (bool, error))`、`FlatMapErr[U any](f func(T) ([]U, error))`、`PeekErr(f func(T) error)`。首错发生：当前 stage 记录错误并令 `Accept` 返回 false（与短路机制同路），下游 `End()` 正常收尾，Collector/累积结果保持一致。
3. **终端侧**：`Err() error`——任意终止操作之后调用，返回求值过程中首个错误；无错误返回 nil。出错时终止操作返回**已累积的部分结果**（与 Scanner 一致，文档明示）。
4. **错误存储**：求值时创建共享错误槽，求值结束写回"发起终止调用的那个 Stream 实例"，`Err()` 从该实例读取。

### 明确不做的（保持简单路径简单）

- 普通算子（`Map`/`Filter`/...）回调**不**带 error 返回值：纯变换场景零错误噪声（对齐 `slices` 包风格）。
- Collector 的 Accumulator/Finisher 不引入错误签名：保持可组合性；fallible 汇聚用"先 `Collect` 到中间容器再校验"表达。

## API 设计详案（三层清单）

### Tier A：必做（Java Stream 对齐）

**源（构造函数，全部惰性）**：`Of`/`FromSlice`（零拷贝引用）/`FromSeq(iter.Seq)`/`FromChannel`/`FromMap[K,V] → *Stream[KV[K,V]]`（Unordered）/`FromFunc(next func() (T, bool, error))`（错误记录）/`Generate`（无限）/`Iterate`（无限）/`Range[I Integer] → *NumberStream[I]`（区间元素必然是数值，直接返回数值流，泛型推断随接收者自动收窄，`Sum(Range(0,100))` 等既有用法不变）/`RangeClosed[I Integer]`（闭区间 [start, stop]，start > stop 得空流）/`Concat`（正名形态为方法 `a.Concat(b)`，包级为 deprecated adapter，见「形态原则与方法化迁移」）/`Empty`/`OfNonZero[T comparable]`（过滤零值元素的可变参数源——zero 涵盖 nil；Java 9 ofNullable 的 Go 惯用法）。

**无状态中间**：`Filter`/`Map[U]`/`FlatMap[U]`/`FlatMapSeq[U]`/`Peek`/`TakeWhile`/`DropWhile`。

**有状态中间**：`Limit(n)`（短路）/`Skip(n)`（`Skip(0)` 恒等返回原流——不新增物化层、特征位透传、不触发并行降级；负参 panic，n==0 为唯一 no-op 特例，语义与 JDK `skip(0) returns this` 一致）/`Sorted(cmp func(a,b T) int)`（不稳定 pdqsort，对齐 `slices.SortFunc`，默认选择）/`StableSorted(cmp)`（稳定，对齐 `slices.SortStableFunc`，等键保持相遇顺序；不设包级自然序 StableSorted——`StableSorted(cmp.Compare[T])` 已覆盖，免过度展开 API 面）/`DistinctBy[K comparable](key)`/`Reverse`。包级 `WindowSliding[T any](s, n int) *Stream[[]T]` 滑动窗口——只输出满窗，元素不足 n 无输出，对齐 Java Gatherers.windowSliding。

**终止**：`ForEach`/`ForEachUntil(f func(T) bool)`/`ToSlice`/`ToSeq() iter.Seq[T]`（出站适配——流交给 range-over-func 或任何接受 `iter.Seq` 的 API，详见「流扩展第一批」）/`Count`/`Reduce(identity, op)`/`ReduceOpt(op) (T, bool)`/`Collect[A,R]`/`First`/`FindAny`（顺序下同 First）/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err()`。

**Collector 族**：`ToSlice`/`ToSet`/`ToMap`（last-wins）/`ToMapMerge`/`GroupingBy`（保遇序）/`Joining`/`Counting`/`Reducing`/`Mapping`/`Summing`/`Averaging`，以及组合生态 `GroupingByDownstream`/`PartitioningBy`/`Teeing`/`Filtering`/`FlatMapping`/`CollectingAndThen`/`MinBy`/`MaxBy`/`Summarizing`/`SummaryStats`（详见「流扩展第一批」Requirement）。

**Splitterator**：`TryAdvance(f func(T) bool) bool`/`ForEachRemaining`/`TrySplit()`/`EstimateSize()`/`Characteristics()`；特征位常量 SpSized/SpOrdered/SpSubSized/SpSorted/SpDistinct/SpLimited（Sp 前缀避免与 `Distinct` 等包级标识符冲突）；实现：slice（可二分）、range（可二分）、seq、channel、func 源（后三者不可分）。

### Tier B：推荐新增（Go 风格 / 泛型方法 showcase）——全部纳入

| API | 形态 | 理由 |
|---|---|---|
| `Enumerate[T](s) *Stream[KV[int, T]]` / `Chunk[T](s, n int) *Stream[[]T]` | 包级函数 | for-index 习惯 / 批处理（写库/分页）高频；均因返回 T 派生类型触发实例化循环 |
| `Scan[U](seed, f)` | 方法 | 滚动累积/前缀和（含初值共 n+1 项）；有状态但单遍无需物化 |
| `Zip[U, R](o, f)` | 泛型方法 | 双流拉链；双类型参数方法是 Go 1.27 泛型方法的最佳 showcase |
| 方法 `Join`（InnerJoin）/`LeftJoin`（左外） | 泛型方法 ×2 | SQL 式条件连接，与 Zip 按位置配对互补（见「双流条件连接 Join」） |
| `FromFunc(next)` | 构造 | 拉式 IO 源 + 错误即值入口（错误模型闭环） |
| `MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr` | 方法 | 错误即值核心 |
| 包级 `Contains`/`Sorted`/`Min`/`Max`/`Sum`/`Avg`/`Distinct`（各自带 `comparable`/`cmp.Ordered`/`Number` 约束） | 包级函数 | Go 泛型方法约束限制的必要补偿（方法无法约束接收者 T）+ 免写比较器/数值聚合的便捷形态，与方法版互补 |
| `NumberStream[N Number]` 数值流 | 嵌入 `Stream[N]` 的具型包装 + 收窄入口 | 对标 Java IntStream 的**约束收窄**（非装箱规避），使需要元素约束的 API 回归方法形态（见「NumberStream 数值流」） |

### Tier C：明确不做（附理由）

- 原始特化流族（IntStream 等）——Go 泛型零装箱无此需求，约束收窄价值由 `NumberStream` 承接；流上 `iterator()` 双向遍历——无场景（单向出站适配 `ToSeq()` 已覆盖 range-over-func 互通）。
- Collector 错误化 Finisher——破坏组合简洁性；限速/背压——channel 源天然具备，库层不掺和。

## 包结构

原则：**只拆与入口包低耦合的部分，无法干净拆分的不强行划分**。

- `constraints` 子包：数值约束 `Integer`/`Float`/`Number` 的零依赖叶子包；根包以类型别名保留 `stream.Integer` 等公开形态（别名同一类型，既有用法零破坏）。子包（如 collector 的 Summing）可复用数值约束而不反向依赖根包。
- `collector` 子包：`Collector[T,A,R]` 与全部预置/组合收集器。对根包**零非导出依赖**、零引擎依赖（不触碰 `pipeline`/`Sink`/`evalCtx`）；仅依赖标准库 `strings` 与 `constraints`（无 import 环）。调用方式 `s.Collect(collector.GroupingBy(k, v))`（用户按需 import 子包）。
- 根包一体（引擎与算子群）：`op.go`/`pipeline.go`/`parallel.go` 三方非导出符号循环互访、`terminal.go` 并行终端依赖非导出接口并直读 `collectingSink.buf`、`op_ext.go`/`parallel.go` 对 `evalCtx` 字段级裸访问——强行拆分须导出全部内部符号或整体下沉 `internal`（等价重做公共接口），收益低于成本，维持单包。

## Impact（文件布局约定）

新增文件需在本节补记：

  - 核心类型与引擎：`stream.go`（Stream/KV/约束别名）、`pipeline.go`、`sink.go`、`spliterator.go`、`op.go`、`sources.go`、`construct.go`、`parallel.go`、`lifecycle.go`（OnClose/Close/Cache）
  - 算子：`ops_stateless.go`（含 Err 变体与 MapToNumber）、`ops_stateful.go`（含 Scan/Chunk）、`op_ext.go`（Zip/Enumerate/WindowSliding/Join/LeftJoin）、`numeric.go`（包级数值族）、`number_stream.go`、`time_window.go`（TimeWindow/SortedByTime/CompleteTimeBuckets）、`terminal.go`（含 Err()/ToSeq 与并行终端）
  - 子包：`constraints/constraints.go`、`collector/collector.go`、`collector/collector_combine.go`（组合生态）
  - 测试：`*_test.go`、`example_test.go`、`benchmark_test.go`、`parallel_test.go`、`whitebox_test.go`、`fuzz_test.go`、`coverage_extra_test.go`、`time_window_test.go` 等
  - 示例：`example/`（独立 module + replace 指向根模块，隔离覆盖率）——`basics`/`collectors`/`numeric`/`errors`/`parallel`/`lifecycle`/`join`/`extensions`/`timewindow`
  - 文档：`README.md`/`README_CN.md`、`docs/design.md`、`docs/api.md`、`skills/go-stream/SKILL.md`（面向下游用户的 coding-agent 使用指引，英文编写、与 API 面同步维护——AGENTS.md 项目专属约定已载明）
  - 协作规范：`AGENTS.md`、`agents/common.md`、`agents/go.md`（`agents/` 下两文件可整体复用到其它项目）
  - CI/质量与发布：`.github/workflows/{ci,govulncheck,scorecard,sonarcloud,codeql,release-please}.yml`、`codecov.yml`、`sonar-project.properties`（Actions 测试矩阵 + lint、Codecov、govulncheck、OpenSSF Scorecard、SonarCloud 质量门禁、CodeQL）、`release-please-config.json`、`.release-please-manifest.json`、`CHANGELOG.md`（release-please 基于 Conventional Commits 自动维护，v0.1.0 及之前条目人工回填）

## ADDED Requirements

### Requirement: Stream 构造（源适配）
系统 SHALL 提供包级构造函数，从多种容器/生成器类型构建 `*Stream[T]`，构造本身不触发任何遍历（惰性）：`Of`/`FromSlice`（零拷贝引用）/`FromSeq`/`FromChannel`/`FromMap`（产出 `KV[K,V]`，Unordered——不声明 `SpOrdered`）/`FromFunc`（错误记录）/`Generate`（无限）/`Iterate`（无限）/`Range`（左闭右开，返回 `*NumberStream[I]`）/`RangeClosed`（闭区间）/`Concat`（方法 `a.Concat(b)` 正名）/`Empty`/`OfNonZero`。

#### Scenario: 无限源 + 短路
- **WHEN** 用户执行 `Generate(f).Limit(5).ToSlice()`
- **THEN** 正常终止并返回 5 个元素

#### Scenario: 拉式源错误
- **WHEN** `FromFunc(next)` 在第 3 次调用返回错误，用户 `ToSlice()` 后调 `Err()`
- **THEN** `ToSlice()` 返回前 2 个元素，`Err()` 返回该错误

### Requirement: 中间操作（惰性、返回新 Stream）
无状态（单遍融合）：`Filter`/`Map[U]`/`FlatMap[U]`/`FlatMapSeq[U]`/`Peek`/`TakeWhile`（短路）/`DropWhile`；Err 变体：`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr`；标志改写：`Unordered()`（清除 `SpOrdered`，声明后续求值不需保序——并行流式合并的门控；不改变元素流）。
有状态（物化上游段）：`Limit`（短路）/`Skip`（`Skip(0)` 恒等返回原流）/`Sorted`（不稳定）/`StableSorted`（稳定）/`DistinctBy`/`Reverse`；单遍有状态（不物化）：`Scan`；包级单遍有状态（实例化循环限制）：`Chunk`/`Enumerate`/`WindowSliding`。
双流：`Zip[U, R]`（取短，两条流均被消费）；`Join`/`LeftJoin`（条件连接，见「双流条件连接 Join」Requirement）。

#### Scenario: 无状态链单遍融合
- **WHEN** 对 N 元素源执行 `.Filter(p).Map(f).Count()`
- **THEN** 源只遍历一次，f 仅对通过 p 的元素调用

#### Scenario: 有状态操作分段
- **WHEN** 执行 `.Filter(p).Sorted(cmp).Map(f).ToSlice()`
- **THEN** 先物化排序（稳定）再单遍流过 map

#### Scenario: Err 变体短路
- **WHEN** `MapErr(f)` 中第 k 个元素转换出错
- **THEN** 源遍历立即停止，下游收到部分元素并正常 End()，`Err()` 返回该错误

### Requirement: 终止操作
`ForEach`/`ForEachUntil`/`ToSlice`/`ToSeq`/`Count`/`Reduce`/`ReduceOpt`/`Collect[A,R]`/`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err() error`；短路：`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`。

#### Scenario: AllMatch 短路
- **WHEN** 对 `[2,4,1,8]` 执行 `.AllMatch(even)`
- **THEN** 遇到 1 即返回 false，不遍历 8

### Requirement: Collector 汇聚抽象
`collector.Collector[T,A,R]` **接口**（Supplier/Accumulator/Combiner/Finisher；Combiner 以「返回合并函数、可为 nil」表达并行支持与否，nil 时 `Collect` 自动降级串行。决策依据：struct 导出函数字段有被外部意外改写的风险，接口 + 非导出具体类型实现保证行为只读；接口派发性能经基准回测无劣化），位于低耦合子包 `stream/collector`；预置收集器见 Tier A 清单。

#### Scenario: 分组保序
- **WHEN** `Of(p1,p2,...).Collect(collector.GroupingBy(p.Id, p.Name))`
- **THEN** 返回 `map[ID][]string` 正确分组且组内保持遇序

### Requirement: Splitterator 抽象与特征位
接口五方法 + 特征位；slice/range 可二分 TrySplit（前后半段不重叠、并集完整）；seq/channel/func 不可分（返回 nil）。特征位沿管道传播，与 Java StreamOpFlag 的 flag 表对齐，**置位必为真**：

- **1:1 变换**（Map/MapErr）：保留 Sized（下游可按 size 预分配），清 Sorted/Distinct。
- **子集/前缀变换**：Filter/FilterErr 清 Sized（子集数量不再精确，`Begin` 的 size 参数本定义为估计数）；TakeWhile/DropWhile 清 Sized；Limit 透传 Sorted/Distinct（前缀保序保异）。
- **1:N 变换**（FlatMap 族、Chunk、WindowSliding）：清 Sized/Sorted/Distinct。
- **元素集不变的置换**（Sorted/StableSorted、DistinctBy、Reverse）：互不清对方位；Reverse 保留 Sorted——`SpSorted` 语义为「按某比较器有序」，升序流反转后按取反比较器仍有序（本库特征位不携带比较器上下文，异于 Java SORTED 绑定自然序的清除语义）。
- **物化型算子**（Limit/Skip/Sorted/StableSorted/DistinctBy/Reverse/TimeWindow）：置 Sized+SubSized（输出=缓冲，总量已知）。
- **Concat**：Ordered 双侧 AND（任一侧序不确定则整体不确定）；其余特征位按位或。

**SpLimited 有限性声明**：区分「已知有限」与「无限或大小未知」。置位规则（仅已知有限）：`SpSized ⇒ SpLimited` 不变式（Of/FromSlice/Empty/Range/RangeClosed）；`FromMap` 置位（len 已知有限、不报大小）；`FromFunc`/`FromSeq`/`FromChannel`（大小未知，库无法替调用方断言）与 `Generate`/`Iterate`（设计上无限）不置位。传播规则：透传类算子（Filter/Map/Peek/Err 族/TakeWhile/DropWhile/Scan/Chunk/FlatMap 族/WindowSliding/标志类）自然保留；物化类强制置位（求值能完成即有限，Limit 给出上界；TimeWindow 桶数物化后已知，输出可作 Join/LeftJoin 右流）；Concat/Join/LeftJoin 双侧 AND（任一侧无限/未知即整体未知）；**Zip 取短 OR**（任一侧已知有限则输出必有限）。消费方：Join/LeftJoin 以之作右流有限性守卫（见「双流条件连接 Join」Requirement）。

### Requirement: 错误即值模型
可预期错误（FromFunc/Err 族）以 error 值传播：首错短路、部分结果保留、`Err()` 查询；不可恢复错误（重复消费、nil 回调）panic 且信息清晰；回调 panic 原样传播。

#### Scenario: 重复消费
- **WHEN** 对同一流两次 `ToSlice()`
- **THEN** 第二次 panic，提示流已被消费

### Requirement: 组合式架构
`Stream[T]` 嵌入 `pipeline[T]`；算子以构造函数 + wrap 闭包实现（无类继承层次）；Splitterator 实现嵌入 `baseSplitterator[T]`；库内不得出现"模拟抽象类待覆写"的基类型。

### Requirement: 包级便捷函数
`Contains[T comparable]`/`Sorted/Min/Max[T cmp.Ordered]`/`Sum/Avg[T Number]`/`Distinct[T comparable]`。与 `NumberStream` 的方法形态并存≠重复：元素约束收窄到包装类型后方法形态合法，包级函数继续服务普通 `*Stream[T]`。

#### Scenario: 数值聚合
- **WHEN** `stream.Range(0, 100).Sum()`（方法形态）
- **THEN** 返回 4950；包级形态 `stream.Sum(普通流)` 继续可用

### Requirement: NumberStream 数值流（Task 18）

对标 Java IntStream 的**约束收窄**形态（非装箱规避——Go 泛型零装箱）：`NumberStream[N Number]` **值嵌入** `Stream[N]`，把元素约束收窄到包装类型自有类型参数位（关键约束第 4 条解法），使需要元素约束的 API 回归方法形态并支持链式调用。

- **收窄入口（上游，返回 \*NumberStream）**：`Range[I Integer](start, stop I)`（区间元素必然是数值，直接返回数值流；既有 `stream.Sum(stream.Range(…))` 调用点迁移为 `.Sum()` 方法链或 `.AsStream()` 桥接）、`OfNumber[N Number](xs ...N)`/`FromNumberSlice[N Number](s []N)`（Number 中缀命名：单词后缀 `Number`，双词中插 `Number`）、`(*Stream[T]).MapToNumber[N Number](f func(T) N)`（类型迁移入窄流，对应 Java `mapToInt`）、`AsNumber[N Number](s *Stream[N])`（通用桥接：复制句柄 + 立即标记原流消费；二次桥接 panic；nil 容错返回 nil）。
- **核心方法（NumberStream 自有，19 个）**：元素保持中间 7（Filter/Peek/TakeWhile/DropWhile/Limit/Skip〔n==0 恒等返回自身〕/Reverse）+ 自然序收窄 3（`Sorted()`/`StableSorted()`/`Distinct()`——免比较器，**遮蔽** Stream 比较器版，自定义比较器经 `AsStream()` 使用）+ 标志/生命周期 4（Parallel/Sequential/Unordered/OnClose）+ 收窄终端 5（`Sum()`/`Avg()`/`Min()`/`Max()`/`Contains(target)`，Min/Max 同样遮蔽比较器版）。
- **逃逸规则**：未覆写的提升方法保持 Stream 语义（类型迁移算子 `Map[U]`/`FlatMap` 族/`Scan`/`Zip` 等自然返回 `*Stream`；值终端 `ToSlice`/`Count`/`First`/`Collect`/`Err` 等直接可用）；同型显式出口 `AsStream() *Stream[N]`（复制句柄 + 标记本流消费；供 Zip 另一侧、`Chunk`/`Enumerate` 等包级函数复用；禁止裸 `&ns.Stream` 别名——别名共享会绕过一次性语义）。
- **一次性语义**：与 Stream 完全一致（链接/桥接即消费，重复使用 panic fail-fast）。
- **构造开销注记**：每级窄链算子 +1 次句柄分配（约 +35% 纯构造耗时），求值热路径持平——重构造轻求值场景先以 `*Stream` 串联、末步 `AsNumber` 收窄（godoc 与 docs/api.md 性能注记已载明）。

#### Scenario: 数值链一行闭环
- **WHEN** `stream.Range(1, 101).Filter(偶数谓词).Sum()`
- **THEN** 返回 2550，全程方法链无包级前缀

#### Scenario: 桥接一次性语义
- **WHEN** `ns := stream.AsNumber(s)` 后再次 `stream.AsNumber(s)` 或继续链接 `s`
- **THEN** panic（编程错误 fail-fast）

### Requirement: 并行求值 v1（Task 8）
`Parallel(n)` 设置并行度、`Sequential()` 还原串行（均为中间操作语义：消费上游、返回携带标志的新流）。求值时满足以下条件才走并行路径，否则自动降级串行（正确性优先）：

- **分片机制**：pipeline 携带类型擦除的 `splitN` 闭包（沿链传播，可穿越 Map 等异构 stage——Go 无 raw type，无法以同型字段存源）；仅可分源（slice/range，即 TrySplit 非 nil 的源）在构造时设置。求值时递归 `TrySplit` 至 n 份（保序：前/后半段递归）；递归中不可再分的子源以自身为一份（元素不丢失，份数可少于 n）；完全不可分返回单份，由 evaluateParallel 据份数 <2 降级串行。
- **分片求值**：每片 goroutine 独立重入 `p.drive`（head 层经 `ec.partSrc` 覆盖源），**每片全新 sink 链 + 独立终端累积**（避免共享 sink 的数据竞争）；物化分片结果后按分片序回放进用户终端（Ordered 保序；无序流走先完成先推的流式合并）。
- **Collect 专属路径**：片级独立 `Supplier`+`Accumulator`，按分片序 `Combiner` 合并，`Finisher` 收尾。
- **降级规则**（splitN 置 nil 或 evaluateNP）：物化型有状态算子（Limit/Skip/Sorted/StableSorted/DistinctBy/Reverse）之后、单遍有状态（Scan/Chunk/Enumerate/DropWhile）、双流（Zip/Concat/Join）、短路终止族（First/FindAny/AnyMatch/AllMatch/NoneMatch/ForEachUntil——保持串行短路优势）、不可分源——均串行。`Skip(0)` 恒等返回原流，不构成物化层，不触发降级（splitN 保留）。
- **错误与 panic**：片内首错按片序合并进主错误槽（部分结果保留）；片内回调 panic 捕获后由发起 goroutine 原样 re-panic。
- **验证**：`go test -race` 全绿；CPU 密集场景并行加速比 benchmark > 1.5x（实测 ~3.3x，4 分片）。

#### Scenario: 并行保序
- **WHEN** `FromSlice(0..9999).Parallel(4).Filter(p).Map(f).ToSlice()`
- **THEN** 结果与串行完全一致（分片序回放）

#### Scenario: 管道含状态算子自动降级
- **WHEN** `FromSlice(xs).Parallel(4).Sorted(cmp).ToSlice()`
- **THEN** 正确排序（串行求值），不 panic

### Requirement: 生命周期与可重放（Task 10）

**1. OnClose/Close 资源管理**：`OnClose(f func() error) *Stream[T]` 注册清理回调（中间操作语义：消费上游、返回携带回调链的新流）；f 出错以 error 值记入错误槽（可经 `Err()` 查询）；nil 回调 panic。`Close() error` 显式关闭（幂等，重复调用不重复触发；未求值流也可关闭）。触发时机：**终止求值结束时自动触发一次**（正常耗尽/短路/错误路径均触发）；未求值即 Close 则自动触发不发生（以显式 Close 为准）。多个回调按注册序执行，任一出错记首错。

**2. Cache 可重放工厂（不破坏一次性模型）**：`func (s *Stream[T]) Cache() func() *Stream[T]`——首次调用工厂时求值上游一次并物化，此后每次调用返回**全新的独立一次性流**（FromSlice 共享底层数组，零拷贝）；原流被 Cache 消费；物化期上游出错 → 首错记录进工厂，此后每次返回携带该错误的空流（`Err()` 可查）。包级 `Cache(s)` 为 deprecated adapter（见「形态原则与方法化迁移」）。

**3. Unordered 流式合并（并行终端优化）**：`SpOrdered` 特征位缺失（无序流）时，分片结果**先完成先推**（无序流式合并），降低端到端延迟。无序流 = 天然无序源（`FromMap`）或经 `Unordered()` 显式声明（清除 SpOrdered 的标志改写中间操作，对应 Java `BaseStream.unordered()`）。FromMap 源不可分（并行仍降级串行），实际生效路径为「可分源 + `Unordered()`」。适用终端：`ToSlice`/`ForEach`/`Min`/`Max`（元素级先完成先推）与 `Collect`（片级 `Combiner` 按完成序合并）；`Count`/`Reduce` 无增量推入语义，仍按片序聚合。语义保证：无序流下结果集合与串行一致（顺序不保证——本就是 Unordered 语义）。

#### Scenario: 求值结束自动释放
- **WHEN** `FromChannel(ch).OnClose(release).ToSlice()` 完成（含短路路径）
- **THEN** release 被调用恰好一次

#### Scenario: Cache 重放
- **WHEN** `f := s.Cache()`; `f().Count()` 先后执行两次
- **THEN** 上游只被求值一次，两次 Count 结果一致（deprecated 包级 `Cache(s)` adapter 行为等同）

#### Scenario: Unordered 流式合并
- **WHEN** `FromSlice(xs).Parallel(4).Unordered().Collect(c)`（可分源 + 显式 Unordered）
- **THEN** 任一分片完成即可推入下游，不等待全部片；结果集合与串行一致

### Requirement: 流扩展第一批（Task 19~23，对齐 Java 25 Stream 能力缺口）

**1. ToSeq 出站适配（Task 19）**：`(*Stream[T]).ToSeq() iter.Seq[T]`——终止求值语义（调用即消费本流），把流编译为 Go 1.23 push 迭代器，供 `for v := range s.ToSeq()` 或任何接受 `iter.Seq` 的 API（如 `slices.Collect`）。消费方提前 break 即短路（`yield` 返回 false → 引擎停止推动源）；错误即值语义保留（`Err()` 可查首错）；OnClose 回调链随求值结束照常触发。同一 `iter.Seq` 值的第二遍 range 将 panic（fail-fast，与全库一次性契约一致）；`NumberStream` 经提升直接可用。

**2. Collector 组合生态（Task 20）**：`GroupingByDownstream[K comparable, T, A, R any](keyF func(T) K, downstream Collector[T, A, R])`——两级汇聚（分组后每组交 downstream 收集，对应 Java `groupingBy(classifier, downstream)`），Combiner 支持并行按组合并，组内保持遇序；`PartitioningBy[T, A, R](p func(T) bool, downstream)`/`PartitioningBySlice[T](p)`——布尔分组（`Partition[T, R]{True, False R}`，False 侧恒非 nil、空组为 downstream 零值结果）；`Teeing[A1, R1, A2, R2, T, R](c1, c2, merge)`——一次遍历同时喂两个下游收集器，结束以 merge 合并双结果（对应 Java 12 teeing；Combiner 双侧均支持并行时才支持，任一 nil 则整体 nil → 串行降级）；`Filtering(p, downstream)`（先过谓词再交下游，对应 Java 9 filtering）；`FlatMapping(f, downstream)`（先 1:N 展开再交下游）；`CollectingAndThen(c, finish)`（finisher 包装）；`MinBy(cmp)`/`MaxBy(cmp)`——收集器形态的最值（空流返回零值，与终端 Min/Max 的 (T, bool) 形态区分）。累积容器均非导出。

**3. WindowSliding 滑动窗口（Task 21）**：包级 `WindowSliding[T any](s *Stream[T], n int) *Stream[[]T]`——窗口逐元素滑动，只输出满窗（长度恰 n），元素少于 n 无输出（对齐 Java Gatherers `windowSliding(n)`）；`n <= 0` panic；nil 流返回 nil。环形缓冲单遍有状态 → 并行降级；特征位同 Chunk（清 Sized/Sorted/Distinct）；包级形态原因同 Chunk（方法返回 `Stream[[]T]` 触发实例化循环）。

**4. Summarizing/SummaryStats 单遍统计（Task 22）**：`collector.SummaryStats[N Number]` 结构（`Count int64`/`Sum N`/`Min N`/`Max N`/`Avg() N`——空流 Avg 返回 0；`String()` 便于打印）、`collector.Summarizing[N Number]()`（单遍同时累积 count/sum/min/max，Combiner 支持并行合并——Go 泛型单收集器覆盖全部数值类型）、根包便捷终端 `Summary[N Number](s *Stream[N]) SummaryStats[N]`（免 import 子包，委托 Summarizing 实现）。

**5. RangeClosed / OfNonZero（Task 23）**：`RangeClosed[I Integer](start, stop I) *NumberStream[I]`——闭区间 [start, stop]（含两端），`start > stop` 得空流（对齐 JDK rangeClosed，与 Range 左闭右开并存），返回数值流（与 Range 一致）；`OfNonZero[T comparable](xs ...T) *Stream[T]`——过滤零值元素的可变参数源（`T comparable` 使零值比较编译期合法；nil 即引用类型零值，数值 0/空串同为零值被过滤）。

#### Scenario: ToSeq 接入 range-over-func
- **WHEN** `for v := range FromSlice(xs).Map(f).ToSeq()` 且中途 break
- **THEN** 已遍历元素正确产出，源遍历随 break 短路停止，`Err()` 返回 nil

#### Scenario: 滑动窗口只出满窗
- **WHEN** `stream.WindowSliding(stream.Of(1,2,3,4), 2).ToSlice()`
- **THEN** 返回 `[[1 2] [2 3] [3 4]]`；元素少于 n（如 `WindowSliding(Of(1), 2)`）无输出

### Requirement: 双流条件连接 Join（Task 24）

将两条流按**谓词条件**连接（逻辑类似 SQL Join），与 Zip 按位置配对互补：`on(t, u) bool` 返回 true 表明当前元素对可以组合，`combiner(t, u) R` 对可组合的元素对执行合并。两个方法形态：

- **方法 `func (s *Stream[T]) Join[U, R any](other *Stream[U], on, combine) *Stream[R]`** —— **InnerJoin**：只有命中 `on` 的元素对才产出 `combine` 结果；左侧元素若无任何命中则不产出。
- **方法 `func (s *Stream[T]) LeftJoin[U, R any](…)`** —— **左外连接**：左侧元素即使无任何命中，也至少执行一次 `combine`（右元素取 `U` 的零值）；有命中则对每个命中的右元素各产出一条。

**不提供 RightJoin**：等价于以右流作为左流调用 `LeftJoin`（`right.LeftJoin(left, …)`）。

设计要点：

- **求值形态（嵌套循环，单侧物化）**：right 流物化为 `[]U`（经 collectingSink 全量收集，含首错短路），再单遍驱动 left 流；对每个左元素扫描全部右元素执行 `on` 判定与 `combine`。**产物顺序确定性**：外层按左流相遇序、内层按右流物化序（Join 不额外引入不确定性）。
- **有限性守卫**：链接期检查 `right.chars&SpLimited == 0` 即 panic（fail-fast，把无限右流的运行期挂死提前为构造期明确报错；置位/传播规则见「Splitterator 抽象与特征位」，`FromFunc`/`FromSeq`/`FromChannel` 作右流同样被拦，需先 `.Limit(上界)` 或换左流）。left 侧与输出侧短路语义正常：左流 Accept 返回 false 即停，此时 right 已全量物化属预期（与 `Sorted` 等物化算子一致——既有物化算子不加守卫，仅随 Join 诞生引入，此不对称为有意决策）。
- **一次性语义**：两条流均被标记消费（checkLinked）；Join 产物为新的一次性流。
- **nil 回调 / nil 流 panic**（编程错误，与 Zip 一致；方法接收者语境下 nil 另一侧更可能是编程错误）。
- **特征位与并行**：输出特征位为两侧按位与后清 `SpSized`/`SpSubSized`/`SpSorted`/`SpDistinct`（元素数为乘性/选择相关）；SpLimited 双侧 AND；splitN 置 nil（双流算子并行降级，与 Zip/Concat 同列）。
- **错误即值**：物化 right 期间或驱动 left 期间的首错均记入共享 evalCtx（首错保留、短路、部分结果保留）；用户回调 panic 原样传播（全程发起 goroutine 内完成，无后台中转）。
- **OnClose 回调链**：经 mergeClosers 继承双侧（left 先 right 后，与 Zip 同规则）。
- **放置位置**：`op_ext.go`（双流算子同置）；`NumberStream` 经提升自然可用（返回 `*Stream[R]`，属逃逸规则既定行为）。

#### Scenario: LeftJoin 未命中走零值
- **WHEN** 左元素在右流无任何 `on` 命中
- **THEN** 该元素仍产出**恰好一条** `combine(t, U 零值)` 结果（按左流位置）；命中元素产出全部命中对

#### Scenario: 右流未声明有限被拦截
- **WHEN** 以 `Generate`/`Iterate`（设计无限）或 `FromFunc`/`FromSeq`/`FromChannel`（大小未知）构建的流作 Join/LeftJoin 的右流（未经物化算子）
- **THEN** 链接时（求值前）panic，信息指明三条出路：换左流 / 右流 `.Limit(上界)` / 先物化；`Generate(…).Limit(n)` 等已置位 SpLimited 的流正常通过

#### Scenario: RightJoin 以 LeftJoin 表达
- **WHEN** 需要 RightJoin 语义
- **THEN** 调用方执行 `right.LeftJoin(left, …)`（右流作为方法接收者/左角色），库不提供第三形态

### Requirement: 形态原则与方法化迁移（Task 25）

项目形态原则：**仅在实现上受 Go 1.27 泛型方法硬限制（需约束接收者 T / 需返回 T 的派生类型）的 API 才用包级函数，其余尽量方法化**。存量盘点：仅 `Concat` 与 `Cache` 可方法化——方法形态无需新增类型参数、不动接收者 T 约束、不返回 T 的派生类型；`Distinct`/`Contains`/`Sorted`/`Min`/`Max`/`Sum`/`Avg`/`Summary` 需约束 T、`Chunk`/`Enumerate`/`WindowSliding` 返回 T 的派生类型（实例化循环），均维持包级（数值族方法形态由 `NumberStream` 承载）。

- **方法化实现**：`func (s *Stream[T]) Concat(other *Stream[T]) *Stream[T]`（construct.go）/ `func (s *Stream[T]) Cache() func() *Stream[T]`（lifecycle.go），原逻辑整体迁入、语义零变化（Concat 的 nil 容错保持：nil 接收者返回 other、other 为 nil 返回本流）。
- **deprecated adapter 策略（平滑迁移）**：旧包函数**保留签名**、实现改为一行委托新方法；godoc 标 `Deprecated:`（指明新形态与移除计划）+ `//go:fix inline` 指令（`go fix` 可自动重写同文件/包内调用点；Go 1.27 的 inline fixer 不跨包，下游模块调用点需手工迁移，文档已说明）。**本版本仅 deprecated（旧签名可编译、行为一致），下一版本移除并标 BREAKING**。
- **仓内零调用**：库代码、测试、example 全部调用点已迁方法形态，deprecated 函数在仓内零引用（staticcheck SA1019 清洁）。

#### Scenario: 旧调用点编译期提示
- **WHEN** 下游代码调用包级 `stream.Concat(a, b)` 或 `stream.Cache(s)`
- **THEN** 编译通过且行为正确，IDE/lint 呈现 Deprecated 提示并指向方法形态；下一版本升级为编译失败（移除）并标 BREAKING

### Requirement: 时间窗口重采样（Task 26）

固定时间间隔的翻转窗口（tumbling window，桶互不重叠、对齐时间网格），与 `WindowSliding`（逐元素滑动）/`Chunk`（定长计数分组）互补，补齐「按时间而非按元素个数分窗」的重采样缺口：

- 包级 `TimeWindow[T any](s *Stream[T], ts func(T) time.Time, d time.Duration) *Stream[TimeBucket[T]]`：以 `ts(v).Truncate(d)` 为桶键把元素分入对齐时间网格的窗口桶，语义为「time.Truncate 桶化 + GroupBy」——**桶输出顺序为桶键首现序、桶内保持相遇序**（对齐 `collector.GroupingBy` 保遇序）；乱序/晚到元素并入其桶键对应的既有桶（桶不拆分）；**不产空桶**（无元素的窗口不存在，补空窗由配套算子显式表达）。**桶键经 `.UTC()` 规范化**：`time.Truncate` 以绝对时间网格对齐（等值瞬间的不同 Location 表示截断后仍等值），但 `time.Time` 作 map 键按结构体 `==`（含 Location 指针）判等，混合时区数据的同一瞬间否则会被拆成两桶——规范化后**同一瞬间恒落同一桶，`Start` 恒为 UTC 网格点**（本地时区网格/展示由调用方对 `Start` 做 `.In(loc)` 后处理）。`TimeBucket[T]{Start time.Time; Items []T}` 为导出桶类型（Start 即桶键，Items 为桶内元素按相遇序的切片）。
- **桶级聚合不设独立入口**：由 `Map` 组合表达——桶内内联聚合（求和/均值/计数），或对 `b.Items` 以 `FromSlice` 建子流交任意 collector（见 `time_window_example_test.go`）。
- **配套包级 `SortedByTime[T any](s *Stream[TimeBucket[T]])`**：按 `Start` 升序的免比较器形态（方法 `Sorted` 需自写比较器；自然序包级 `Sorted` 要求 `cmp.Ordered`，结构体不满足）。决策依据：原始采样数据通常已时间升序（TimeWindow 输出即时间序，零额外开销的常见路径），不为低频乱序场景增加默认开销；备选「要求输入流时间有序」被否决——`SpSorted` 的有序不必是时间有序（特征位无法表达键域语义），运行期校验又违背链接期惰性契约。**文档必须明示**：上游时间乱序时 TimeWindow 输出非时间序，需时间序请接 `SortedByTime`。桶键唯一，等键元素不存在，稳定性无差异。
- **配套包级 `CompleteTimeBuckets[T any](s *Stream[TimeBucket[T]], d time.Duration)`**：对已按时间升序的桶流，在相邻桶空档内按 d 步进插入空桶（`Items` 为 nil），输出保持升序。**范围数据驱动**（首桶之前/末桶之后不补——时间范围是调用方域，库不越权推断）；**结构补全与填值分离**（空桶不携带值，补零/前值等填值语义由后接 `Map` 等算子组合表达，对齐 pandas resample+fillna / Polars upsample+fill_null 的分工）；输入必须升序（乱序输入语义未定义，先接 `SortedByTime`）；溢出护栏 `maxTimeBuckets`（1<<20）：补全桶数超限 panic，防御宽度 d 与数据时间尺度失配（如误传毫秒宽给跨月数据）的天量分配。**文档必须明示**：TimeWindow 不产空桶，需要连续时间轴/补零时接 `CompleteTimeBuckets`。
- **实现为独立新文件 `time_window.go`**：内联「物化 → 变换回放」两段式（协议同 `newStateful`，但不改其既有签名）；物化型有状态 → 并行降级（splitN 不继承）、不支持无限源（配合 Limit 先行截断可用）；特征位置 SpSized/SpSubSized（桶数物化后已知）、SpLimited（物化型强制置位，输出可作 Join/LeftJoin 右流）、清 SpSorted/SpDistinct（分组重排顺序关系）；上游出错（错误即值）时不产出任何桶，`Err()` 可查。
- **参数契约**：`ts == nil` / `d <= 0` panic（对齐 WindowSliding/nil 回调惯例）；`s == nil` 返回 nil。包级形态原因（实测复现）：方法返回 `Stream[TimeBucket[T]]`（`TimeBucket[T]` 含 `Items []T`，为 T 的派生类型）触发 Go 1.27 实例化循环，同 Chunk/WindowSliding 之因。

#### Scenario: 乱序晚到并入既有桶
- **WHEN** 时间序为 [t3, t1, t3]（t1 与 t3 分属不同桶，d 整分）执行 `TimeWindow(s, ts, d)`
- **THEN** 输出 2 桶（t3 桶含 2 个元素、t1 桶 1 个），t3 桶不被拆分、桶序为 [t3, t1]

#### Scenario: 混合时区表示的等值瞬间并入同桶
- **WHEN** 元素时间戳为同一瞬间的不同 Location 表示（如 UTC 与 +08:00）执行 `TimeWindow(s, ts, d)`
- **THEN** 输出单桶（Start 为 UTC 网格点）；桶键以 `.UTC()` 规范化

#### Scenario: 乱序上游 + 时间序需求
- **WHEN** 上游时间乱序（TimeWindow 输出为键首现序、非时间序），流接 `SortedByTime`
- **THEN** 输出按 `Start` 升序，各桶 Items 不变

#### Scenario: 空档补全
- **WHEN** 升序桶流 Start 为 [t0, t0+3d]（中间两窗无元素）执行 `CompleteTimeBuckets(s, d)`
- **THEN** 输出 [t0, t0+d（空桶）, t0+2d（空桶）, t0+3d]——空桶 Items 为 nil、原桶 Items 不变、输出保持升序；首桶之前与末桶之后不补

#### Scenario: 上游出错不产出
- **WHEN** FromFunc 源在第 k 个元素返回错误，流经 `TimeWindow`
- **THEN** 输出为空（变换不执行）、Begin(0)/End 配对、`Err()` 返回首错

### Requirement: 质量保障
**文档交付**：`README.md`/`README_CN.md`（简介/安装/快速上手/API 速览/与 Java 对照/设计要点/路线图）、`docs/design.md`（架构原理：管道/Sink/Splitterator/分段求值/错误模型/组合替代继承映射表/并行求值）、`docs/api.md`（分组 API 参考 + 示例）；`example_test.go` 可运行示例与文档示例一致。
全部公开 API 中文 godoc；`go fix`/`gofmt`/`go vet`/`go test ./...`/`golangci-lint` 全绿（命令清单见 `agents/go.md`）；**语句覆盖率 100% 基线**（根包与 collector 子包，`go tool cover -func` 总计）；benchmark：`Filter+Map+ToSlice` 相对手写 for 循环额外开销目标 <3x；并行求值 `go test -race` 全绿；Fuzz 测试锁定核心不变量（分片并集==原集合且保序、随机算子组合与参考实现等价、并行与串行等价、Zip 取短、Join 等价、Cache 重放、TimeWindow 分桶等价），语料不随仓提交。

### Requirement: 示例目录（example/，Task 15）

提供独立于测试内 Example 函数的**完整可运行示例目录** `example/`：每个示例为独立的 `package main`，`go -C example run ./<名称>` 即可直接编译运行；用户可整文件复制进自己的项目改用。示例**不受 README 篇幅限制**，覆盖典型场景全量 API。现有九个：`basics`/`collectors`/`numeric`/`errors`/`parallel`/`lifecycle`/`join`/`extensions`/`timewindow`。

**覆盖率例外（强制）**：`example/` 为可执行示例而非测试代码，以**独立 Go module**（`example/go.mod` + `replace` 指向根模块）承载——Go 1.22+ 会把无测试文件的包以 0% 计入 coverprofile，嵌套模块从根模块的 `./...` 中彻底隔离，根模块覆盖率基线不受影响；CI 增加独立步骤对 example 模块执行 `go vet`/`go build`/golangci-lint（示例保持可编译、不烂尾）；SonarCloud 按文件系统分析（不受 module 边界影响），`sonar-project.properties` `sonar.exclusions` 排除 `example/**`；根包 `example_test.go` 的 Example 函数仍照常运行（属根模块测试，不受本例外影响）。

#### Scenario: 示例不影响覆盖率
- **WHEN** 在根模块运行 `go test -coverprofile ./...` 并 `go tool cover -func` 汇总
- **THEN** 覆盖率统计不包含 `example/` 目录（100% 基线不变）

## 非目标（Non-Goals）
见 Tier C「明确不做」清单。
