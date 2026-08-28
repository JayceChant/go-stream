# Tasks

依赖关系：Task 1 → Task 2 → (Task 3 ∥ Task 4 ∥ Task 5) → Task 6 → Task 7（文档可与测试并行）

- [x] Task 1: 项目脚手架与核心类型定义（组合式架构）
  - [x] SubTask 1.1: 初始化 go module `github.com/JayceChant/go-stream`（go 1.27），根包 `stream`
  - [x] SubTask 1.2: 定义 `Sink[T]` 接口（`Begin(size int64)`/`Accept(T) bool`/`End()`），Accept 返回 bool 融合取消语义
  - [x] SubTask 1.3: 定义 `Splitterator[T]` 接口、`Characteristics` 位标志（Sized/Ordered/SubSized/Sorted/Distinct）、`baseSplitterator[T]` 公共嵌入 struct（默认 TrySplit 返回 nil）
  - [x] SubTask 1.4: 定义 `pipeline[T]` struct（drive 求值闭包〔构造期捕获上游引用与 wrap，因 Go 无 raw type 无法以同型字段存放异构上游〕/source/chars/consumed/err 错误槽）与 `type Stream[T any] struct { pipeline[T] }`（嵌入组合）
  - [x] SubTask 1.5: 定义 `KV[K,V]`、`Number` 约束（Integer | Float）
  - [x] 验证：`go build ./...` 通过；接口定义不含方法级类型参数（Go 1.27 接口限制）

- [ ] Task 2: 求值引擎（pipeline 核心 + 错误即值机制）
  - [ ] SubTask 2.1: `newStateless(up, wrap func(down Sink[T]) Sink[T])`：仅记录 wrap 闭包，不遍历
  - [ ] SubTask 2.2: `newStateful(up, materialize, wrap)`：求值时先驱动上游物化为 `[]T`，再以 slice splitterator 续段（物化闭包签名为并行 TODO 预留）
  - [ ] SubTask 2.3: `evaluate`：从终止 sink 反向 wrapSink 逐级包装至源；`copyInto` 推动源；短路（Accept 返回 false）即停；错误槽记录首错并短路（与短路同路）
  - [ ] SubTask 2.4: 一次性消费检查：中间操作链接上游时与终止求值时均置 consumed，重复使用 panic（信息清晰）
  - [ ] SubTask 2.5: 错误槽写回"发起终止调用的 Stream 实例"，`Err()` 读取
  - [ ] 验证：单测——空流、Filter 链只遍历一次（计数器探针）、重复消费 panic、Err 短路后部分结果保留

- [ ] Task 3: 构造函数与 Splitterator 实现
  - [ ] SubTask 3.1: `Of`/`FromSlice`(零拷贝)/`Empty`/`FromSeq`/`FromChannel`/`FromMap`(KV, Unordered)/`FromFunc(next func() (T, bool, error))`/`Generate`/`Iterate`/`Range[I Integer]`/`Concat`
  - [ ] SubTask 3.2: splitterator 实现：slice（可二分 TrySplit）、range（可二分）、seq、channel、func 源（不可分）；生成器型 EstimateSize 返回 -1
  - [ ] 验证：单测各源正确性、TrySplit 前后半段不重叠且并集完整、FromFunc 错误记录与部分结果

- [ ] Task 4: 中间操作（含 Err 变体与扩展算子）
  - [ ] SubTask 4.1: 无状态：`Filter`/`Map`(泛型方法)/`FlatMap`/`FlatMapSeq`/`Peek`/`TakeWhile`/`DropWhile`
  - [ ] SubTask 4.2: Err 变体：`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr`（首错短路 + 错误槽记录）
  - [ ] SubTask 4.3: 有状态（物化型）：`Limit`(短路)/`Skip`/`Sorted`(稳定, cmp int 比较器)/`DistinctBy`/`Reverse`
  - [ ] SubTask 4.4: 有状态（单遍型，不物化）：`Scan[U]`/`Chunk(n)`/`Enumerate`；双流：`Zip[U,R]`（取短，两流均消费）
  - [ ] SubTask 4.5: 特征位传播（Map 清除 Sized/Sorted/Distinct；Filter 保留 Sized；物化后 SubSized）
  - [ ] 验证：单测各操作语义（空流/超界/稳定排序/DistinctBy 首见/Scan 前缀和/Chunk 尾块/Zip 取短）、短路链（无限源+TakeWhile/Limit）、MapErr 短路与 Err() 返回

- [ ] Task 5: 终止操作与 Collector
  - [ ] SubTask 5.1: `ForEach`/`ForEachUntil`/`ToSlice`/`Count`/`Reduce`/`ReduceOpt`/`First`/`FindAny`/`AnyMatch`/`AllMatch`/`NoneMatch`/`Min(cmp)`/`Max(cmp)`/`Err()`
  - [ ] SubTask 5.2: `Collector[T,A,R]` struct（Supplier/Accumulator/Combiner/Finisher + IdentityFinish/Unordered 特征）与 `Collect[A,R]` 泛型方法
  - [ ] SubTask 5.3: 预置收集器：ToSlice/ToSet/ToMap(last-wins)/ToMapMerge/GroupingBy(保遇序)/Joining/Counting/Reducing/Mapping
  - [ ] SubTask 5.4: 包级便捷函数：`Contains[T comparable]`/`Sorted/Min/Max[T cmp.Ordered]`/`Sum/Avg[T Number]`/`Distinct[T comparable]`
  - [ ] 验证：单测全部终止操作（短路计数探针）、收集器（分组保序、ToMap last-wins、ToMapMerge 合并）、`stream.Sum(stream.Range(0,100)) == 4950`

- [ ] Task 6: 端到端测试与基准
  - [ ] SubTask 6.1: 端到端组合场景（filter→map→collect、groupingBy、无限流 take、错误管道部分结果等，模拟 Java 典型用法）
  - [ ] SubTask 6.2: benchmark：管道 vs 手写 for 循环（1e2/1e4/1e6 规模），记录开销倍数
  - [ ] SubTask 6.3: `go vet` 清洁、全部公开 API 中文 godoc
  - [ ] 验证：`go test ./...` 全绿；benchmark 目标 <3x

- [ ] Task 7: Markdown 文档与可运行示例
  - [ ] SubTask 7.1: `README.md`：简介/安装/快速上手/API 速览/与 Java 对照表/设计要点/路线图（**明确列出并行 `Parallel(n)` TODO**）
  - [ ] SubTask 7.2: `docs/design.md`：架构原理（管道/Sink/Splitterator/分段求值/错误即值模型/组合替代继承映射表）
  - [ ] SubTask 7.3: `docs/api.md`：按源/中间/终止/Collector 分组的 API 参考 + 示例
  - [ ] SubTask 7.4: `example_test.go`：可运行示例，与文档示例保持一致
  - [ ] 验证：`go test` 执行 example 通过；文档示例可复制运行

# 后续 TODO（本阶段不实现，下一 spec 必做）
- [ ] Task 8（后续）: 并行求值 `Parallel(n)`/`Sequential()`
  - [ ] TrySplit 递归分片至 n 份，goroutine 各跑管道，Collector.Combiner 合并
  - [ ] Ordered 特征按分片序合并；短路终止竞速
  - [ ] `go test -race ./...` 全绿 + 并行加速比 benchmark
  - 依赖：阶段 1 的 TrySplit/特征位/Combiner/物化闭包签名已稳定

# Task Dependencies
- [Task 2] depends on [Task 1]
- [Task 3]、[Task 4]、[Task 5] depends on [Task 2]（三组可并行开发）
- [Task 6] depends on [Task 3][Task 4][Task 5]
- [Task 7] depends on [Task 5]（API 稳定后编写），可与 [Task 6] 并行
- [Task 8（后续）] depends on [Task 6][Task 7]（整体稳定后另立 spec）
