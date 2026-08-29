# Tasks

依赖关系：Task 1 → Task 2 → (Task 3 ∥ Task 4 ∥ Task 5) → Task 6 → Task 7（文档可与测试并行）

- [x] Task 1: 项目脚手架与核心类型定义（组合式架构）
  - [x] SubTask 1.1: 初始化 go module `github.com/JayceChant/go-stream`（go 1.27），根包 `stream`
  - [x] SubTask 1.2: 定义 `Sink[T]` 接口（`Begin(size int64)`/`Accept(T) bool`/`End()`），Accept 返回 bool 融合取消语义
  - [x] SubTask 1.3: 定义 `Splitterator[T]` 接口、`Characteristics` 位标志（Sized/Ordered/SubSized/Sorted/Distinct）、`baseSplitterator[T]` 公共嵌入 struct（默认 TrySplit 返回 nil）
  - [x] SubTask 1.4: 定义 `pipeline[T]` struct（drive 求值闭包〔构造期捕获上游引用与 wrap，因 Go 无 raw type 无法以同型字段存放异构上游〕/source/chars/consumed/err 错误槽）与 `type Stream[T any] struct { pipeline[T] }`（嵌入组合）
  - [x] SubTask 1.5: 定义 `KV[K,V]`、`Number` 约束（Integer | Float）
  - [x] 验证：`go build ./...` 通过；接口定义不含方法级类型参数（Go 1.27 接口限制）

- [x] Task 2: 求值引擎（pipeline 核心 + 错误即值机制）
  - [x] SubTask 2.1: `newStateless(up, wrap func(down Sink[T], ec *evalCtx) Sink[T])`：构造期组合 drive 求值闭包（捕获上游），不遍历
  - [x] SubTask 2.2: `newStateful(up, limit, process)`：求值时第一段驱动上游物化进 collectingSink（limit 可截断，支持无限源短路收集），第二段经 process 变换后单遍回放（原"materialize+wrap 双闭包"收敛为 limit+process 两点式，续段协议由引擎统一处理）
  - [x] SubTask 2.3: `evaluate`：终止求值入口（一次性检查/创建 evalCtx/执行 drive/错误写回）；短路（Accept 返回 false）即停；错误槽记录首错并与短路同路（`ec.fail` 一并表达）
  - [x] SubTask 2.4: 一次性消费检查：中间操作链接上游时与终止求值时均置 consumed，重复使用 panic（统一文案 errConsumed）
  - [x] SubTask 2.5: 错误槽写回"发起终止调用的 Stream 实例"（`Err()` 读取在 Task 5 终止操作实现）
  - [x] 验证：单测——基本流转 Begin/End 配对、Filter+Map 链单遍（计数器探针）、短路停止推动源、limit 截断、重复消费/重复链接 panic、Err 短路后部分结果与 End 收尾

- [x] Task 3: 构造函数与 Splitterator 实现
  - [x] SubTask 3.1: `Of`/`FromSlice`(零拷贝)/`Empty`/`FromSeq`/`FromChannel`/`FromMap`(KV, Unordered)/`FromFunc(next func() (T, bool, error))`/`Generate`/`Iterate`/`Range[I Integer]`/`Concat`
  - [x] SubTask 3.2: splitterator 实现：slice（可二分 TrySplit）、range（可二分，溢出安全中点）、seq（iter.Pull 拉取式，支持单步推进与取消释放）、channel、func 源（不可分）；生成器型 EstimateSize 返回 -1
  - [x] 验证：单测各源正确性、TrySplit 前后半段不重叠且并集完整、FromFunc 错误记录与部分结果

- [x] Task 4: 中间操作（含 Err 变体与扩展算子）
  - [x] SubTask 4.1: 无状态：`Filter`/`Map`(泛型方法)/`FlatMap`/`FlatMapSeq`/`Peek`/`TakeWhile`/`DropWhile`
  - [x] SubTask 4.2: Err 变体：`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr`（首错短路 + 错误槽记录）
  - [x] SubTask 4.3: 有状态（物化型）：`Limit`(短路)/`Skip`/`Sorted`(稳定, cmp int 比较器)/`DistinctBy`/`Reverse`
  - [x] SubTask 4.4: 有状态（单遍型，不物化）：`Scan[U]`（方法）；`Chunk(n)`/`Enumerate` 因 Go 1.27 泛型方法实例化循环限制（T→[]T/KV[int,T] 派生类型）改为**包级函数**；双流：`Zip[U,R]`（取短，pullFromDrive 后台拉取 + stop 防泄漏）
  - [x] SubTask 4.5: 特征位传播（常量重命名 SpSized/SpOrdered/SpSubSized/SpSorted/SpDistinct 避免与 Distinct 函数冲突）：**修订（Task 6）**——Map/MapErr 1:1 变换保留 SpSized（对齐 Java StreamOpFlag，使下游按 size 预分配），仅清 SpSorted/SpDistinct；FlatMap 族与 TakeWhile/DropWhile 清 SpSized；Filter 保留；Limit/Skip/物化后置 SpSized+SpSubSized
  - [x] 验证：单测各操作语义（空流/超界/Limit(0) 边界/稳定排序/DistinctBy 首见/Scan 前缀和含初值/Chunk 尾块/Zip 取短与无限源停止/Zip panic 传播）、短路链（无限源+TakeWhile/Limit）、MapErr 短路与 Err() 返回、特征位传播断言、nil 回调 panic、`go test -race` 全绿

- [x] Task 5: 终止操作与 Collector
  - [x] SubTask 5.1: `ForEach`/`ForEachUntil`/`ToSlice`/`Count`/`Reduce`/`ReduceOpt`/`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err()`
  - [x] SubTask 5.2: `Collector[T,A,R]` struct（Supplier/Accumulator/Combiner/Finisher + IdentityFinish/Unordered 特征）与 `Collect[A,R]` 泛型方法
  - [x] SubTask 5.3: 预置收集器：ToSlice/ToSet/ToMap(last-wins)/ToMapMerge/GroupingBy(保遇序)/Joining/Counting/Reducing/Mapping
  - [x] SubTask 5.4: 包级便捷函数：`Contains[T comparable]`/`Sorted/Min/Max[T cmp.Ordered]`/`Sum/Avg[T Number]`/`Distinct[T comparable]`
  - [x] 验证：单测全部终止操作（短路计数探针）、收集器（分组保序、ToMap last-wins、ToMapMerge 合并）、`stream.Sum(stream.Range(0,100)) == 4950`

- [x] Task 6: 端到端测试与基准
  - [x] SubTask 6.1: 端到端组合场景（filter→map→collect、groupingBy、无限流 take、错误管道部分结果等，模拟 Java 典型用法）
  - [x] SubTask 6.2: benchmark：管道 vs 手写 for 循环（1e2/1e4/1e6 规模），记录开销倍数
    - 实测（Ryzen 5 7535U，benchtime 300ms）：Filter+Map+ToSlice（字符串）开销倍数 1e2=2.84x / 1e4=2.61x / 1e6=1.56x，全部 <3x 达标
    - 附 BenchmarkPipelineIntVsManual（纯数值无分配）为引擎裸开销参考，不计入验收
  - [x] SubTask 6.3: `go vet` 清洁、全部公开 API 中文 godoc
  - [x] 验证：`go test ./...` 全绿；benchmark 目标 <3x
  - [x] 附带（spec 修订）：Map/MapErr 1:1 变换保留 SpSized（对齐 Java StreamOpFlag；原规则清 SpSized 致下游无法预分配，1e2 规模 4.8x 超标，修正后 2.84x）

- [x] Task 7: Markdown 文档与可运行示例
  - [x] SubTask 7.1: `README.md`：简介/安装/快速上手/API 速览/与 Java 对照表/设计要点/路线图（**明确列出并行 `Parallel(n)` TODO**）
  - [x] SubTask 7.2: `docs/design.md`：架构原理（管道/Sink/Splitterator/分段求值/错误即值模型/组合替代继承映射表）
  - [x] SubTask 7.3: `docs/api.md`：按源/中间/终止/Collector 分组的 API 参考 + 示例
  - [x] SubTask 7.4: `example_test.go`：可运行示例，与文档示例保持一致
  - [x] 验证：`go test` 执行 example 通过（9 个 Example 全 PASS）；文档示例可复制运行

# 后续 TODO（已随本 goal 完成）
- [x] Task 8: 并行求值 `Parallel(n)`/`Sequential()`
  - [x] TrySplit 递归分片至 n 份（类型擦除 splitN 闭包沿链传播），goroutine 各跑管道，Collector.Combiner 合并
  - [x] Ordered 特征按分片序合并（物化回放保序）；短路终止族与物化/单遍有状态/双流算子后自动降级串行
  - [x] `go test -race ./...` 全绿 + 并行加速比实测 ~3.3x（4 分片，CPU 密集）
  - [x] spec 语义细化见「并行求值 v1」Requirement；README/docs 同步
  - 依赖：阶段 1 的 TrySplit/特征位/Combiner/物化闭包签名已稳定（已复用）

- [x] Task 9: 子包划分（只拆低耦合部分，不强行划分）
  - [x] 耦合度分析（证据见 spec「包结构」）：collector.go 零非导出依赖可拆；pipeline/op/parallel 等 12 文件循环互访不拆
  - [x] 新建 `collector` 子包：Collector 类型 + 9 个预置收集器迁移（零依赖叶子包）；子包独立单测 collector_test.go
  - [x] 例外处理：Summing 依赖根包 Number 约束，留根包 collector.go（与 Sum/Avg 数值族同置）
  - [x] 根包适配：Stream.Collect 签名改 collector.Collector；全部测试改跨包调用
  - [x] spec「包结构」章节 + README/docs/api.md/design.md 同步

# 后续 TODO（Task 10，随「实现路线图遗留项」goal 立项）
- [x] Task 10: 路线图遗留项（OnClose/Cache/Unordered 流式合并）
  - [x] `OnClose(f func() error) *Stream[T]` / `Close() error`：回调链求值结束自动触发（含短路/错误路径）、显式幂等、按注册序、出错记首错
  - [x] `Cache[T any](s *Stream[T]) func() *Stream[T]` 可重放工厂：首次调用物化一次，此后 FromSlice 零拷贝全新流；物化期首错记忆，此后每次返回携带错误的空流
  - [x] `Unordered() *Stream[T]` 标志改写中间操作（清 SpOrdered）+ 并行终端无序流式合并（先完成先推）：ToSlice/ForEach/Min/Max 元素级、Collect 片级 Combiner 按完成序；Count/Reduce 仍片序聚合
  - [x] FromMap 特征位修正（不再声明 SpOrdered——此前经 newSeqSp 误置，与本源无序语义矛盾）
  - [x] 单测三项语义 + `go test -race` 全绿；README 路线图勾选、docs/api.md、spec/checklist.md 增项
  - 依赖：Task 8（并行求值）、Task 9（包结构稳定）

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3]、[Task 4]、[Task 5] depends on [Task 2]（三组可并行开发）
- [Task 6] depends on [Task 3][Task 4][Task 5]
- [Task 7] depends on [Task 5]（API 稳定后编写），可与 [Task 6] 并行
- [Task 8（后续）] depends on [Task 6][Task 7]（整体稳定后另立 spec）
