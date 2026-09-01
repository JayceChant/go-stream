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

# 后续 TODO（Task 11，随「测试强化 + 引入 fuzzy」goal 立项）
- [x] Task 11: 测试强化（白盒补缺）+ 引入 Fuzz 测试
  - [x] 白盒补缺（whitebox_test.go，按覆盖审计缺口）：
    - [x] `splitSrc`/`splitNOf` 直接单测：n<2 整体一份、n==2 快路径、奇数 n 不对称递归、深递归（n 超可分能力返回少于 n 份）、不可分源单份、子源并集==原集合且保序
    - [x] 并行片内 panic 捕获与 re-panic（有序 total / 无序流式两路径）
    - [x] `newStateful` 错误路径：上游出错时 process 不执行、Begin(0)/End 仍配对
    - [x] `Concat` a 段出错：b 段不驱动、End 恰一次、Err 为 a 侧首错；suppressEnd/skipBegin 直测
    - [x] `evaluateParallel` 运行期降级（splitN 非 nil 但返回单份）与 `newFlagStage` nil splitN 包装（降级算子后再 Parallel）
    - [x] `streamTotal.pushPart` 取消返回 false 路径白盒直测；`sliceTotal.total` 回放短路
    - [x] 源级路径：seqSp 取消释放（stop 调用）、channelSp 短路不排空、funcSp 首错缓存早退
    - [x] `collectingSink` 容量断言：size>0 预分配、limit<size 截断、size<=0 不分配、limit=0 首次即拒
    - [x] `evalCtx` 并发：多 goroutine fail 首错唯一性；takePanic 一次性语义（二次调用 nil）
    - [x] 特征位传播矩阵补全（Peek/FlatMapSeq/Err 变体/Skip 强制置位/DistinctBy/Reverse 透传/单遍有状态四算子/Concat/Zip/Parallel 透传/各源特征位）+ splitN 降级断言 + Collect 无 Combiner 并行降级 + 有序并行 Min/Max
  - [x] 修复（审计与 fuzz 设计发现的真实 bug）：
    - [x] `splitSrc` 递归深处不可再分子源被整段丢弃（[1,2,3].Parallel(4) 丢 [1]；2 元素 Parallel(4) 丢半）——修复为以自身为一份，元素不丢失
    - [x] `sliceTotal.total` 回放短路 `break` 仅断本片继续下一片（取消语义破坏）——改 `return` 停止全部后续回放
    - [x] `newRangeSp` 漏置 SpSubSized（与 newSliceSp 不一致）
  - [x] Fuzz 测试（fuzz_test.go，7 目标锁定内部不变量与等价性；语料不入库）：
    - [x] FuzzSplitSrcInvariant：子源并集==原集合且保序、份数边界（n<2 恰一份 / 可分源至少 2 份 / ≤n）
    - [x] FuzzCollectingSinkBoundary：min(limit,count) 边界与元素按序保全
    - [x] FuzzPipelineEquivalence：Filter→Map→Limit→Skip→条件 Reverse 与参考实现等价
    - [x] FuzzParallelEquivalence：有序并行逐元素一致、无序集合一致、Count 一致、Collect 分组计数一致
    - [x] FuzzZipShortest：min 长度与逐对配对
    - [x] FuzzChunkEnumerate：定长分组尾组、展平复原、索引配对
    - [x] FuzzCacheReplayEquivalence：只求值一次、三轮重放一致
  - [x] 质量门槛：gofmt/vet/`go test -race -count=1 ./...` 全绿；7 个 fuzz 目标各 10s 实跑零 crash（FuzzSplitSrcInvariant 67 万次执行）
  - 依赖：Task 8/10（并行与流式合并已稳定）

# 后续 TODO（Task 12，随「引擎命名可读性优化」goal 立项）
- [x] Task 12: drive 链可读性优化（不改语义与求值表示，评估「结构体 + 指针」方案后维持闭包组合）
  - [x] 引入命名类型 `driveFunc[T]`：stage 求值闭包告别裸 func 签名；`driveFromSource`/`driveFuncErr`/`pullFromDrive` 签名同步
  - [x] 算子构造局部变量弃缩写（ud/sd/ad/bd → driveUpstream/driveSelf/driveA/driveB）
  - [x] 补充「后级包前级的类型必然性」洞察（调用方持有最后一级 Stream[R] ⇒ drive 元素类型被 R 锁死 ⇒ 包裹方向不可反转）：design.md §2 / pipeline.go 注释 / spec.md AbstractPipeline 映射行
  - 依赖：无（纯可读性改动）

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3]、[Task 4]、[Task 5] depends on [Task 2]（三组可并行开发）
- [Task 6] depends on [Task 3][Task 4][Task 5]
- [Task 7] depends on [Task 5]（API 稳定后编写），可与 [Task 6] 并行
- [Task 8（后续）] depends on [Task 6][Task 7]（整体稳定后另立 spec）

# 后续 TODO（Task 13，随「提高测试覆盖率」goal 立项）
- [x] Task 13: 语句覆盖率提升至 100%
  - [x] 覆盖审计：以 coverprofile 0 计数块为缺口清单（根包 91.9%→100%、collector 子包 70.3%→100%，0 计数块清零）
  - [x] collector 子包（collector_test.go 增补 TestCombiners）：ToSet/ToMapMerge/GroupingBy/Joining/Counting/Reducing 的 Combiner 并行合并路径（含 Joining 空侧无分隔符分支）
  - [x] 根包新增 coverage_extra_test.go：
    - nil 参数 panic 矩阵 20 项（ForEach/ForEachUntil/Reduce/ReduceOpt/FindAny/AnyMatch/AllMatch/NoneMatch/Min/Max/FlatMapSeq/FilterErr/FlatMapErr/PeekErr/Skip 负参/Sorted/DistinctBy/Scan/Zip 双参）
    - 边界与 nil 容错：Avg 空流、Contains/Sorted/Min/Max/Distinct nil 流、FromSeq(nil)、Concat 单侧 nil、FromMap 短路、Parallel n<=1
    - 源级 TryAdvance 边界：sliceSp/rangeSp（推进/取消/耗尽/剩余遍历）、rangeSp 不可分裂、seqSp done 早退、channelSp 耗尽与 ForEachRemaining 取消
    - 短路穿透：物化回放（Sorted+First）、Scan 种子、FlatMap/FlatMapErr 展开中途取消
    - Err 变体：FilterErr 谓词 false 不短路 + 出错短路、FlatMapErr 正常展开/短路、PeekErr 部分结果
    - 并行收集器：Summing Combiner 并行 Collect；Concat 单侧回调 mergeClosers；sliceTotal total/pushPart 非 collectingSink 容错与取消
  - [x] 质量门槛：gofmt 空 / vet 无告警 / `go test -count=1 ./...` 全绿 / `go test -race -count=1 ./...` 全绿（Linux 侧工具链无 cgo，-race 经 Windows 侧 go.exe 验证）
  - 依赖：无
