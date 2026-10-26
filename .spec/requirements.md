# go-stream 行为契约（EARS）

> 可观测契约（Requirement/Scenario）；实现决策与取舍见同目录 [decisions.md](decisions.md)；组织方式与 EARS/ADR 边界见 [README.md](README.md)。API 签名速查见 [docs/api.md](../docs/api.md)，架构原理见 [docs/design.md](../docs/design.md)。

## 构造与源

### Requirement: Stream 构造（源适配）

系统 SHALL 提供包级构造函数，从多种容器/生成器类型构建 `*Stream[T]`，构造本身不触发任何遍历（惰性）：

- 固定/可枚举源：`Of`、`FromSlice`（零拷贝引用）、`FromSeq`（`iter.Seq`）、`FromChannel`、`FromMap`（产出 `KV[K,V]`，不声明有序）、`OfNonZero`（过滤零值元素；`T comparable`）。
- 生成器源：`Generate`（无限）、`Iterate`（无限）。
- 数值源：`Range`（左闭右开）、`RangeClosed`（闭区间），均返回 `*NumberStream[I]`；`start > stop` 时 `RangeClosed` 为空流。
- 拉式源：`FromFunc(next func() (T, bool, error))` 读取元素并记录首个错误。
- 组合与空源：方法形态 `a.Concat(b)`（包级 `Concat` 为 deprecated adapter）、`Empty`。

**决策依据**：源适配覆盖 Go 常见容器/生成器，惰性构造保证管道可组合并支持无限源；见 [decisions.md](decisions.md)「立项与 Java Stream → Go 设计取舍（v1 → v2）」。

#### Scenario: 无限源 + 短路
- **WHEN** 用户执行 `Generate(f).Limit(5).ToSlice()`
- **THEN** 正常终止并返回 5 个元素

#### Scenario: 拉式源错误
- **WHEN** `FromFunc(next)` 在第 3 次调用返回错误，用户 `ToSlice()` 后调 `Err()`
- **THEN** `ToSlice()` 返回前 2 个元素，`Err()` 返回该错误

### Requirement: 中间操作（惰性、返回新 Stream）

中间操作 SHALL 只声明管道、不触发遍历；求值时无状态算子单遍融合。

- 无状态：`Filter`、`Map[U]`、`FlatMap[U]`、`FlatMapSeq[U]`、`Peek`、`TakeWhile`（短路）、`DropWhile`；错误变体 `MapErr`、`FilterErr`、`FlatMapErr`、`PeekErr`；标志改写 `Unordered()`（声明后续求值不需保序，不改变元素集合）。
- 有状态：`Limit`（短路）、`Skip`（`Skip(0)` 恒等返回原流）、`Sorted`（不稳定）、`StableSorted`（稳定，等键保持相遇序）、`DistinctBy[K comparable]`、`Reverse`；单遍有状态 `Scan[U]`。
- 包级单遍有状态（受泛型方法限制）：`Chunk`、`Enumerate`、`WindowSliding`。
- 双流：`Zip[U, R]`（取短，两条流均被消费）、`Join`/`LeftJoin`（见「双流条件连接 Join」）。

**决策依据**：无状态算子融合保证单遍与低开销，有状态算子按需物化上游段；见 [decisions.md](decisions.md)「求值引擎：闭包组合与单遍融合」「API 形态原则：方法优先、Join 方法化与 deprecated adapter」。

#### Scenario: 无状态链单遍融合
- **WHEN** 对 N 元素源执行 `.Filter(p).Map(f).Count()`
- **THEN** 源只遍历一次，`f` 仅对通过 `p` 的元素调用

#### Scenario: 有状态排序
- **WHEN** 执行 `.Filter(p).Sorted(cmp).Map(f).ToSlice()`
- **THEN** 结果按 `cmp` 排序，`f` 在排序后的元素上各调用一次，源只遍历一次

#### Scenario: Err 变体短路
- **WHEN** `MapErr(f)` 中第 k 个元素转换出错
- **THEN** 源遍历立即停止，下游收到部分元素并正常收尾，`Err()` 返回该错误

### Requirement: 终止操作

终止操作 SHALL 触发一次求值并消费流；短路终端在满足条件后停止推动源。
`ForEach`、`ForEachUntil`、`ToSlice`、`ToSeq() iter.Seq[T]`、`Count`、`Reduce`、`ReduceOpt`、`Collect[A,R]`、`First`、`FindAny`、`AnyMatch`、`AllMatch`、`NoneMatch`、`Min(cmp)`、`Max(cmp)`、`Err() error`。短路：`First`、`FindAny`、`AnyMatch`、`AllMatch`、`NoneMatch`、`ForEachUntil`。

**决策依据**：终止操作是求值边界，短路族使无限源可用；见 [decisions.md](decisions.md)「求值引擎：闭包组合与单遍融合」。

#### Scenario: AllMatch 短路
- **WHEN** 对 `[2,4,1,8]` 执行 `.AllMatch(even)`
- **THEN** 遇到 1 即返回 false，不遍历 8

## 汇聚与数据源

### Requirement: Collector 汇聚抽象

`collector.Collector[T,A,R]` SHALL 为接口，含 Supplier/Accumulator/Combiner/Finisher 四方法；`Combiner` 返回 nil 表示不支持并行合并，`Collect` 自动降级串行。预置收集器与组合生态见 [docs/api.md](../docs/api.md)；`GroupingBy` 组内保持相遇序。

**决策依据**：接口 + 非导出具体类型保证收集器行为只读，避免外部改写内部状态；见 [decisions.md](decisions.md)「Collector 接口化」。

#### Scenario: 分组保序
- **WHEN** `Of(p1,p2,...).Collect(collector.GroupingBy(p.Id, p.Name))`
- **THEN** 返回 `map[ID][]string` 正确分组且组内保持相遇序

### Requirement: Splitterator 抽象与特征位

`Splitterator[T]` SHALL 提供 `TryAdvance`、`ForEachRemaining`、`TrySplit`、`EstimateSize`、`Characteristics` 五方法；`TrySplit` 返回的前后两段 SHALL 不重叠且并集完整，不可分割时返回 nil。slice 与 range 源可二分；seq、channel、func 源不可分。特征位常量 `SpSized`、`SpOrdered`、`SpSubSized`、`SpSorted`、`SpDistinct`、`SpLimited` 沿管道传播且**置位必为真**；`SpLimited` 表示源已知有限。

**决策依据**：特征位让下游按大小预分配，并为并行、短路与 Join 的有限性判定提供依据；见 [decisions.md](decisions.md)「特征位语义审计与 SpLimited 有限性声明」。

#### Scenario: 二分不重叠
- **WHEN** 对 slice 源的 Splitterator 连续 `TrySplit`
- **THEN** 各分段元素互不重叠，并集等于原源，顺序保持

## 错误与 API 形态

### Requirement: 错误即值模型

可预期且可恢复的错误 SHALL 以 error 值传播：首错发生后停止遍历、保留部分结果，终端之后 `Err()` 返回首个错误。不可恢复的编程错误（重复消费流、nil 回调）SHALL panic，信息清晰且为英文；用户回调内部的 panic SHALL 原样传播（不吞、不改语义）。

**决策依据**：错误即值对齐 Go 标准库 Scanner/Rows 惯例，失败路径不引入异常控制流；见 [decisions.md](decisions.md)「错误即值模型」。

#### Scenario: 重复消费
- **WHEN** 对同一流两次 `ToSlice()`
- **THEN** 第二次 panic，提示流已被消费

### Requirement: 包级便捷函数

系统 SHALL 为受泛型方法硬限制的 API 提供包级函数：`Contains[T comparable]`、`Distinct[T comparable]`、`Sorted`/`Min`/`Max[T cmp.Ordered]`、`Sum`/`Avg[T Number]`。它们与 `NumberStream` 的方法形态并存，不视为重复。

**决策依据**：Go 1.27 方法不能约束接收者已有类型参数，元素约束类 API 只能包级；数值流包装类型承接方法形态；见 [decisions.md](decisions.md)「Go 1.27 泛型方法约束与组合替代继承」。

#### Scenario: 数值聚合
- **WHEN** `stream.Range(0, 100).Sum()`（方法形态）
- **THEN** 返回 4950；包级形态 `stream.Sum(普通流)` 继续可用

### Requirement: NumberStream 数值流

`NumberStream[N Number]` SHALL 为嵌入 `Stream[N]` 的数值收窄形态，把元素约束收窄到包装类型自有类型参数位，使需要元素约束的 API 回归方法形态并支持链式调用。

- 收窄入口：`Range`、`RangeClosed`、`OfNumber`、`FromNumberSlice`、`(*Stream[T]).MapToNumber[N Number]`、`AsNumber[N Number]`（通用桥接）。
- 核心方法（19 个）：元素保持类 `Filter`/`Peek`/`TakeWhile`/`DropWhile`/`Limit`/`Skip`（`n==0` 恒等返回自身）/`Reverse`；自然序收窄 `Sorted()`/`StableSorted()`/`Distinct()`（遮蔽 Stream 的比较器版）；标志/生命周期 `Parallel`/`Sequential`/`Unordered`/`OnClose`；收窄终端 `Sum()`/`Avg()`/`Min()`/`Max()`/`Contains(target)`（Min/Max 遮蔽比较器版）。
- 逃逸规则：未覆写的提升方法保持 Stream 语义（类型迁移算子返回 `*Stream`；值终端直接可用）；同型显式出口 `AsStream() *Stream[N]`（复制句柄并标记本流消费；禁止裸别名）。
- 一次性语义与 Stream 一致：链接/桥接即消费，重复使用 panic。

**决策依据**：数值约束收窄而非装箱规避（Go 泛型零装箱）；每级窄链算子有额外句柄分配，构造开销注记见 [docs/api.md](../docs/api.md)；见 [decisions.md](decisions.md)「NumberStream 数值流：约束收窄形态」。

#### Scenario: 数值链一行闭环
- **WHEN** `stream.Range(1, 101).Filter(偶数谓词).Sum()`
- **THEN** 返回 2550，全程方法链无包级前缀

#### Scenario: 桥接一次性语义
- **WHEN** `ns := stream.AsNumber(s)` 后再次 `stream.AsNumber(s)` 或继续链接 `s`
- **THEN** panic（编程错误 fail-fast）

## 求值与生命周期

### Requirement: 并行求值

`Parallel(n)` 设置并行度、`Sequential()` 还原串行（均为中间操作，消费上游并返回携带标志的新流）。求值结果 SHALL 与串行一致；无法安全并行的管道（物化型有状态算子、单遍有状态、双流、短路终端族、不可分源）SHALL 自动降级串行且不 panic。`Unordered()` 流不保证输出顺序，但结果集合与串行一致；错误/panic 语义与串行一致（片内首错合并、回调 panic 原样传播）。

**决策依据**：正确性优先，只在可分源与可并行阶段启用并行；见 [decisions.md](decisions.md)「并行求值 v1：分片、降级与无序流式合并」与 [docs/design.md](../docs/design.md) 第 7 节。

#### Scenario: 并行保序
- **WHEN** `FromSlice(0..9999).Parallel(4).Filter(p).Map(f).ToSlice()`
- **THEN** 结果与串行完全一致

#### Scenario: 管道含状态算子自动降级
- **WHEN** `FromSlice(xs).Parallel(4).Sorted(cmp).ToSlice()`
- **THEN** 正确排序（串行求值），不 panic

### Requirement: 生命周期与可重放

- `OnClose(f func() error)` 注册清理回调（中间操作语义）；`Close() error` 显式关闭（幂等）。终止求值结束（正常耗尽/短路/错误）SHALL 自动触发回调恰好一次；未求值即 `Close` 以显式关闭为准。多个回调按注册序执行，任一出错记首错。
- `Cache() func() *Stream[T]` SHALL 在首次调用时求值并物化上游一次，此后每次返回全新的独立一次性流；原流被消费。物化期错误由此后返回的空流携带，`Err()` 可查。
- `Unordered()` 使分片结果先完成先推（无序流式合并），适用于 `ToSlice`/`ForEach`/`Min`/`Max`/`Collect`；结果集合与串行一致，顺序不保证。

**决策依据**：资源释放与可重放不破坏一次性模型；见 [decisions.md](decisions.md)「生命周期与可重放：OnClose/Close 与 Cache」与 [docs/design.md](../docs/design.md) 第 8 节。

#### Scenario: 求值结束自动释放
- **WHEN** `FromChannel(ch).OnClose(release).ToSlice()` 完成（含短路路径）
- **THEN** release 被调用恰好一次

#### Scenario: Cache 重放
- **WHEN** `f := s.Cache()`; `f().Count()` 先后执行两次
- **THEN** 上游只被求值一次，两次 Count 结果一致

#### Scenario: Unordered 流式合并
- **WHEN** `FromSlice(xs).Parallel(4).Unordered().Collect(c)`（可分源 + 显式 Unordered）
- **THEN** 任一分片完成即可推入下游，不等待全部片；结果集合与串行一致

## 扩展能力

### Requirement: 流扩展第一批

系统 SHALL 提供以下扩展能力：

- `ToSeq() iter.Seq[T]`：终止求值语义，供 `for v := range` 或接受 `iter.Seq` 的 API 使用；消费方提前 break 即短路；`Err()` 保留首错；同一 `iter.Seq` 值的第二遍 range panic。
- Collector 组合生态：`GroupingByDownstream`、`PartitioningBy`/`PartitioningBySlice`（`Partition[T,R]{True,False}`，False 侧恒非 nil）、`Teeing`、`Filtering`、`FlatMapping`、`CollectingAndThen`、`MinBy`/`MaxBy`。
- `WindowSliding[T](s, n)`：逐元素滑动，只输出满窗；`n <= 0` panic；nil 流返回 nil。
- `collector.SummaryStats[N]`（Count/Sum/Min/Max/Avg；空流 Avg 为 0）与 `collector.Summarizing[N]()`；根包便捷终端 `Summary[N](s)`。
- `RangeClosed` 与 `OfNonZero`（见「Stream 构造」）。

**决策依据**：对齐 Java 25 Stream 能力缺口，保持组合式扩展而非独立聚合入口；见 [decisions.md](decisions.md)「流扩展第一批：能力缺口与形态选择」与 [docs/api.md](../docs/api.md)。

#### Scenario: ToSeq 接入 range-over-func
- **WHEN** `for v := range FromSlice(xs).Map(f).ToSeq()` 且中途 break
- **THEN** 已遍历元素正确产出，源遍历随 break 短路停止，`Err()` 返回 nil

#### Scenario: 滑动窗口只出满窗
- **WHEN** `stream.WindowSliding(stream.Of(1,2,3,4), 2).ToSlice()`
- **THEN** 返回 `[[1 2] [2 3] [3 4]]`；元素少于 n（如 `WindowSliding(Of(1), 2)`）无输出

### Requirement: 双流条件连接 Join

`Join`/`LeftJoin` SHALL 按谓词连接两条流：

- 方法 `func (s *Stream[T]) Join[U, R any](other *Stream[U], on func(T,U) bool, combine func(T,U) R) *Stream[R]`：内连接，只产出 `on` 命中的组合。
- 方法 `LeftJoin[U,R]`：左外连接；左侧元素无命中时仍产出恰好一条 `combine(t, U 零值)`，有命中则每个命中右元素各产出一条。
- 不提供 `RightJoin`：以右流作为左流调用 `LeftJoin` 表达。
- 右流 SHALL 已知有限（`SpLimited`）；链接期不满足则 panic，并提示换左流 / 右流 `Limit(上界)` / 先物化。左流保持流式，输出顺序为左流相遇序 × 右流物化序。
- 两条流均被标记消费，产物为新的一次性流；nil 回调/nil 流 panic；错误与回调 panic 语义同其他算子；`OnClose` 继承双侧。

**决策依据**：SQL 式条件连接与 Zip 的位置配对互补；右流物化 + 有限性守卫把运行期挂死提前为链接期错误；见 [decisions.md](decisions.md)「API 形态原则：方法优先、Join 方法化与 deprecated adapter」「特征位语义审计与 SpLimited 有限性声明」。

#### Scenario: LeftJoin 未命中走零值
- **WHEN** 左元素在右流无任何 `on` 命中
- **THEN** 该元素仍产出恰好一条 `combine(t, U 零值)` 结果（按左流位置）；命中元素产出全部命中对

#### Scenario: 右流未声明有限被拦截
- **WHEN** 以 `Generate`/`Iterate`（设计无限）或 `FromFunc`/`FromSeq`/`FromChannel`（大小未知）构建的流作右流（未经物化算子）
- **THEN** 链接时（求值前）panic，信息指明三条出路；`Generate(…).Limit(n)` 等已置位 `SpLimited` 的流正常通过

#### Scenario: RightJoin 以 LeftJoin 表达
- **WHEN** 需要 RightJoin 语义
- **THEN** 调用方执行 `right.LeftJoin(left, …)`，库不提供第三形态

### Requirement: API 形态与兼容性

API SHALL 以方法形态为主；仅在 Go 1.27 泛型方法硬限制（需约束接收者 `T` / 需返回 `T` 的派生类型）下使用包级函数，并在文档中说明原因。`Concat` 与 `Cache` 的包级形态 SHALL 保留为 deprecated adapter（行为一致、编译通过），下一版本移除并标记 BREAKING；仓内调用点 SHALL 全部迁移到方法形态。

**决策依据**：减少 API 面的两种调用风格，deprecated adapter 提供平滑迁移窗口；见 [decisions.md](decisions.md)「API 形态原则：方法优先、Join 方法化与 deprecated adapter」。

#### Scenario: 旧调用点编译期提示
- **WHEN** 下游代码调用包级 `stream.Concat(a, b)` 或 `stream.Cache(s)`
- **THEN** 编译通过且行为正确，IDE/lint 呈现 Deprecated 提示并指向方法形态；下一版本升级为编译失败并标 BREAKING

### Requirement: 时间窗口重采样

- `TimeWindow[T](s, ts, d) *Stream[TimeBucket[T]]` SHALL 以 `ts(v).Truncate(d)` 的 UTC 网格点为桶键分桶：桶输出顺序为桶键首现序、桶内保持相遇序；乱序/晚到元素并入既有桶；不产空桶。同一瞬间的不同时区表示 SHALL 落入同一桶。`TimeBucket[T]{Start time.Time; Items []T}`。
- 桶级聚合不设独立入口，由 `Map` 组合表达。
- `SortedByTime[T](s)` SHALL 按 `Start` 升序输出；上游时间乱序时 TimeWindow 输出非时间序，需接 `SortedByTime`。
- `CompleteTimeBuckets[T](s, d)` SHALL 在已按时间升序的相邻桶空档内按 d 步进插入空桶（`Items` 为 nil），输出保持升序；首桶之前/末桶之后不补；输入乱序语义未定义；补全桶数超过 `1<<20` panic。
- 参数契约：`ts == nil` / `d <= 0` panic；`s == nil` 返回 nil。上游出错时不产出任何桶，`Err()` 可查。

**决策依据**：固定时间网格重采样与按元素滑动/定长分组互补；空桶补全与填值分离；见 [decisions.md](decisions.md)「时间窗口重采样：UTC 桶键、不产空桶与顺序策略」。

#### Scenario: 乱序晚到并入既有桶
- **WHEN** 时间序为 `[t3, t1, t3]`（t1 与 t3 分属不同桶，d 整分）执行 `TimeWindow(s, ts, d)`
- **THEN** 输出 2 桶（t3 桶含 2 个元素、t1 桶 1 个），t3 桶不被拆分、桶序为 `[t3, t1]`

#### Scenario: 混合时区表示的等值瞬间并入同桶
- **WHEN** 元素时间戳为同一瞬间的不同 Location 表示（如 UTC 与 +08:00）执行 `TimeWindow(s, ts, d)`
- **THEN** 输出单桶（Start 为 UTC 网格点）

#### Scenario: 乱序上游 + 时间序需求
- **WHEN** 上游时间乱序（TimeWindow 输出为键首现序、非时间序），流接 `SortedByTime`
- **THEN** 输出按 `Start` 升序，各桶 Items 不变

#### Scenario: 空档补全
- **WHEN** 升序桶流 Start 为 `[t0, t0+3d]`（中间两窗无元素）执行 `CompleteTimeBuckets(s, d)`
- **THEN** 输出 `[t0, t0+d（空桶）, t0+2d（空桶）, t0+3d]`——空桶 Items 为 nil、原桶 Items 不变、输出保持升序；首桶之前与末桶之后不补

#### Scenario: 上游出错不产出
- **WHEN** `FromFunc` 源在第 k 个元素返回错误，流经 `TimeWindow`
- **THEN** 输出为空（变换不执行），终端的 Begin/End 配对，`Err()` 返回首错

## 交付

### Requirement: 示例目录

仓库 SHALL 提供独立可运行示例目录 `example/`：每个示例为独立 `package main`，`go -C example run ./<名称>` 可直接编译运行，用户可整文件复制。现有九个：`basics`、`collectors`、`numeric`、`errors`、`parallel`、`lifecycle`、`join`、`extensions`、`timewindow`。`example/` SHALL 以独立 Go module 承载（`example/go.mod` + `replace` 指向根模块），不得计入根模块覆盖率；CI SHALL 单独 build/vet，SonarCloud 排除 `example/**`。

**决策依据**：可运行示例比测试内 Example 更适合整文件复制，独立 module 避免无测试包拉低覆盖率；见 [decisions.md](decisions.md)「示例目录：独立 module 与覆盖率隔离」。

#### Scenario: 示例不影响覆盖率
- **WHEN** 在根模块运行 `go test -coverprofile ./...` 并 `go tool cover -func` 汇总
- **THEN** 覆盖率统计不包含 `example/` 目录（100% 基线不变）

## 非目标（Non-Goals）

- 原始类型特化流族（IntStream 等）：Go 泛型零装箱，无此需求；约束收窄由 `NumberStream` 承接。
- 流上 `iterator()` 双向遍历：单向出站适配 `ToSeq()` 已覆盖 range-over-func 互通。
- 普通算子回调带 error 签名：纯变换场景保持零错误噪声，fallible 场景用 Err 变体。
- Collector 的 Accumulator/Finisher 引入错误签名：保持可组合性，fallible 汇聚以中间容器 + 校验表达。
- Collector 错误化 Finisher、库层限速/背压：保持简单路径简单；channel 源天然具备背压。

**决策依据**：见 [decisions.md](decisions.md)「立项与 Java Stream → Go 设计取舍（v1 → v2）」。
