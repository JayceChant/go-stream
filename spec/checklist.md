# Checklist

## NumberStream 数值流（Task 18）
- [x] `NumberStream[N Number]` 值嵌入 `Stream[N]`：约束收窄到自有类型参数位，Sum/Avg/Min/Max/Contains/自然序 Sorted/StableSorted/Distinct 回归方法形态
- [x] 收窄入口：`Range` 直接返回 `*NumberStream[I]`（签名修订，旧调用点已迁移）、`OfNumber`/`FromNumberSlice`、`MapToNumber`（Stream 方法）、`AsNumber`/`AsStream` 双向桥接（复制句柄 + 标记消费，一次性 fail-fast，nil 容错）
- [x] 核心 19 方法：元素保持中间 7（Skip(0) 恒等返回自身）+ 自然序 3 + 标志/生命周期 4 + 收窄终端 5；比较器版 Sorted/StableSorted/Min/Max 被遮蔽，经 AsStream 使用
- [x] 逃逸规则：未覆写提升方法保持 Stream 语义（类型迁移自然返回 *Stream、值终端直接可用）
- [x] 单测覆盖 19 方法 + 收窄入口 + 桥接一次性语义 + 并行链；质量门槛全绿（gofmt 空 / vet 无告警 / `go test -race -count=1` 全绿 / golangci-lint 0 issues / 覆盖率总计 100.0%，number_stream.go 全函数 100%）

## 架构与核心机制（组合替代继承）
- [x] Stream 为具体泛型 struct（非接口），中间/终止操作通过 Go 1.27 泛型方法实现（如 `Map[U any]`、`Zip[U, R]`、`Collect[A, R]`），未在任何接口中声明带类型参数的方法
- [x] `Stream[T]` 通过嵌入 `pipeline[T]` 组合核心求值机；算子以"构造函数 + wrap 闭包"实现，无类继承层次、无"模拟抽象类待覆写"的基类型
- [x] Splitterator 各实现嵌入 `baseSplitterator[T]` 复用公共字段与默认 TrySplit
- [x] 中间操作惰性：仅追加管道 stage，不触发遍历、不物化数据
- [x] 无状态操作在终止求值时融合为单遍历（wrapSink 反向包装 + 源单遍推动）
- [x] 有状态物化型操作在求值时先驱动上游段物化为 `[]T` 再续段（分段求值）；`Scan`/`Chunk`/`Enumerate` 单遍有状态、不物化
- [x] 同一 Stream 实例仅可消费/链接一次，重复使用 panic 且错误信息清晰（中间操作链接上游时即检查）
- [x] 短路语义：Limit/First/AnyMatch/TakeWhile 等能提前终止源遍历（无限源测试通过）

## 错误即值模型
- [x] 可预期错误（`FromFunc` 源失败、`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr` 回调返回错误）以 error 值传播：首错短路、部分结果保留、`Err()` 返回首个错误
- [x] 普通算子回调无 error 签名（纯路径零噪声，对齐 `slices` 包风格）
- [x] 不可恢复错误 panic：重复消费、nil 回调；用户回调 panic 原样传播
- [x] `ToMap` 键冲突 last-wins（文档明示），`ToMapMerge` 提供自定义合并

## API 完整性
- [x] 构造函数齐全：Of/FromSlice/FromSeq/FromChannel/FromMap/FromFunc/Generate/Iterate/Range/Concat/Empty
- [x] 无状态中间操作齐全：Filter/Map/FlatMap/FlatMapSeq/Peek/TakeWhile/DropWhile + MapErr/FilterErr/FlatMapErr/PeekErr
- [x] 有状态中间操作齐全：Limit/Skip/Sorted(稳定)/DistinctBy/Reverse + Scan/Chunk/Enumerate + Zip
- [x] 终止操作齐全：ForEach/ForEachUntil/ToSlice/Count/Reduce/ReduceOpt/Collect/First/FindAny/AnyMatch/AllMatch/NoneMatch/Min/Max/Err
- [x] Collector 与预置收集器齐全：ToSlice/ToSet/ToMap/ToMapMerge/GroupingBy/Joining/Counting/Reducing/Mapping/Summing/Averaging；已迁移至低耦合子包 `collector`（无三方依赖，仅共享类型约束）
- [x] Collector 为接口 + 非导出具体类型实现（Task 17：struct 导出函数字段有被外部改写的风险）；Combiner 为「返回合并函数、可为 nil」，nil 时 Collect 自动降级串行；性能经 BenchmarkCollect 回测无劣化（详见 tasks.md Task 17）
- [x] 包级便捷函数齐全：Contains/Sorted/Min/Max/Sum/Avg/Distinct（泛型约束补偿设计）
- [x] Splitterator 接口含 TryAdvance/ForEachRemaining/TrySplit/EstimateSize/Characteristics，特征位齐全且沿管道正确传播
- [x] Map 操作类型迁移静态类型安全（编译期检查）

## 并行（Task 8 已实现）
- [x] TrySplit/EstimateSize/Characteristics 语义完整（slice/range 可二分，前后不重叠并集完整）
- [x] Collector 含 Combiner 方法且全部预置收集器实现分片合并；newStateful 物化闭包签名已用于降级判断
- [x] `Parallel(n)`/`Sequential()`：类型擦除 splitN 分片闭包沿链传播、每片独立 sink 链求值、分片序合并保序、降级规则（物化/单遍有状态/双流/短路终止族/不可分源自动串行）、错误分片语义与 panic re-panic
- [x] README 路线图已更新为已实现（实测 4 分片加速比 ~3.3x）

## 包结构（Task 9 / Task 16）
- [x] 低耦合部分已拆分：`collector` 子包零非导出依赖、零引擎依赖（无 import 环）；`constraints` 叶子包承载公共数值约束（Task 16），collector 与根包均以别名/引用复用
- [x] 无法干净拆分的引擎/算子群未强行划分（三方循环互访、字段级裸访问等证据见 spec「包结构」）；排查确认无其它「仅因公共类型依赖而无法分包」的遗留（Task 16）
- [x] 子包独立单测（不依赖根包，验证叶子包性质）

## 生命周期与可重放（Task 10）
- [x] `OnClose(f func() error)`/`Close() error`：回调链求值结束自动触发（耗尽/短路/错误值/回调 panic 路径均恰好一次）、显式关闭幂等、按注册序执行、出错记首错经 `Err()` 查询、nil 回调 panic
- [x] 回调链沿中间操作继承，Concat/Zip 经 mergeClosers 合并双方（求值序），每物理回调 sync.Once 保证多路径触发恰好一次
- [x] `Cache[T](s) func() *Stream[T]`：上游只求值一次（sync.Once 物化）、工厂产物为全新一次性流（FromSlice 零拷贝）、一次性模型不被破坏、物化期首错记忆（此后返回携带错误的空流，Err() 可查）、工厂未调用则原流仍可用
- [x] `Unordered() *Stream[T]` 标志改写（清 SpOrdered）+ 并行无序流式合并：ToSlice/ForEach/Min/Max 元素级先完成先推（终端取消即停止推送）、Collect 片级 Combiner 完成序合并、Count/Reduce 仍片序聚合、结果集合与串行一致
- [x] FromMap 特征位修正：不再声明 SpOrdered（此前经 newSeqSp 误置，与 map 遍历序不确定的既定语义矛盾）
- [x] `go test -race -count=1 ./...` 全绿（新增 lifecycle_test.go 17 项，含流式合并确定性早推验证）

## 测试强化与 Fuzz（Task 11）
- [x] 白盒补缺 26 项（whitebox_test.go）：splitSrc/splitNOf 全路径（含 n<2 恰一份/奇数/深递归/不可分单份/保序并集）、并行片内 panic re-panic 双路径、newStateful 错误路径 process 不执行、Concat 错误路径与段包装器直测、运行期并行降级与 nil splitN 包装、pushPart 取消直测、sliceTotal 回放短路、三源释放/短路/缓存语义、collectingSink 容量四边界、evalCtx 并发 fail 唯一性与 takePanic 一次性、特征位传播矩阵全覆盖（含 splitN 降级断言）、Collect 无 Combiner 降级、有序并行 Min/Max
- [x] 随测试发现并修复 3 个真实 bug：splitSrc 深递归丢元素（小输入并行元素丢失）、sliceTotal.total 短路 break 仅断本片（取消语义破坏）、newRangeSp 漏置 SpSubSized
- [x] Fuzz 7 目标（fuzz_test.go）：分片不变量、collectingSink 边界、管道等价（Filter/Map/Limit/Skip/Reverse vs 参考实现）、并行等价（有序逐元素/无序集合/Count/分组）、Zip 取短、Chunk/Enumerate、Cache 重放；各 10s 实跑零 crash；语料不入库
- [x] 质量门槛全绿：gofmt 空 / vet 无告警 / `go test -race -count=1 ./...` 全绿

## 示例目录（Task 15）
- [x] `example/` 为嵌套独立 module（`example/go.mod` + replace 指向根模块）：根模块 `go test ./...`/coverprofile 完全不含 example（Go 1.22+ 会把无测试文件的包以 0% 计入，嵌套模块规避之），覆盖率总计保持 100.0%
- [x] 六个可运行示例（`go -C example run ./<名称>`）：basics（构造→中间→终止全流程）、collectors（预置收集器族+自定义 TopN）、numeric（数值聚合/Scan/无限源/Zip/Chunk/Enumerate）、errors（错误即值全路径）、parallel（保序/流式合并/自动降级）、lifecycle（OnClose/Close/Cache）
- [x] 每个示例为独立 `package main`、自包含可复制；输出确定性（map/无序场景先排序）
- [x] CI 独立步骤 `go -C example build/vet`（示例保持可编译）；SonarCloud `sonar.exclusions` 排除 `example/**`；根包 `example_test.go` Example 函数照常运行不受影响

## 文档（Markdown）
- [x] README.md：简介/安装/快速上手/API 速览/与 Java 对照表/设计要点/路线图（并行已实现）
- [x] docs/design.md：架构原理（管道/Sink/Splitterator/分段求值/错误模型/组合替代继承映射表/并行求值/生命周期与可重放）
- [x] docs/api.md：分组 API 参考 + 示例（含并行控制、生命周期与可重放）
- [x] example_test.go 可运行示例（9 个 Example 全 PASS），与文档示例一致

## 质量验证
- [x] `go build ./...` 通过（go 1.27，模块路径 github.com/JayceChant/go-stream）
- [x] `go vet ./...` 无告警
- [x] `go test -race ./...` 全绿（含空流、单元素、超界参数、短路计数探针、TrySplit 不重叠并集完整、`StableSorted` 等键保序、`Sorted` 排序正确性与源不可变、DistinctBy 首见保留、重复消费 panic、Err 短路部分结果、FromFunc 错误、ToMap last-wins、Scan/Chunk/Zip 语义、并行 11 项）
- [x] 排序拆分（spec 修订）：`Sorted` 改不稳定 pdqsort（对齐 `slices.SortFunc`）、新增 `StableSorted`（对齐 `slices.SortStableFunc`）；Top-K 基准同契约重测（不稳定 2.3x/1.1x/2.4x，稳定参考 ~1.2x）
- [x] benchmark 产出：Filter+Map+ToSlice 相对手写 for 循环额外开销 <3x（实测 1e2=2.7x / 1e4=1.8x / 1e6=1.6x）
- [x] 全部公开 API 具备中文 godoc 注释

## 覆盖率提升至 100%（Task 13）
- [x] 覆盖审计：以 coverprofile 0 计数块为缺口清单（此前根包 91.9%、collector 子包 70.3%）
- [x] collector 子包补齐全部 Combiner 并行合并路径（ToSet/ToMapMerge/GroupingBy/Joining/Counting/Reducing，含 Joining 空侧分支）——100%
- [x] 根包新增 coverage_extra_test.go：nil 参数 panic 矩阵 20 项、便捷函数 nil/空流边界、源级 TryAdvance 边界（slice/range/seq/channel）、短路穿透物化回放与 Scan 种子、Err 变体全路径、并行 Summing、mergeClosers 单侧、sliceTotal 类型容错与取消——100%
- [x] 质量门槛全绿：gofmt 空 / vet 无告警 / `go test -count=1 ./...` 全绿 / `go test -race -count=1 ./...` 全绿
- [x] `go tool cover -func` 总计 100%（根包与 collector 子包均 100%，0 计数块清零）

## 流扩展第一批（Task 19~23，对齐 Java 25）
- [x] `ToSeq() iter.Seq[T]` 出站适配（terminal.go）：终止求值语义、break 短路源遍历、错误即值与 OnClose 照常、二次 range panic（一次性契约）、NumberStream 提升可用
- [x] Collector 组合生态（collector_combine.go）：GroupingByDownstream/PartitioningBy+Slice/Teeing/Filtering/FlatMapping/CollectingAndThen/MinBy/MaxBy 九收集器；下游 Combiner 可用则并行合并、任一 nil 整体降级串行
- [x] `WindowSliding` 滑动窗口（op_ext.go）：环形缓冲单遍、只出满窗（不足 n 无输出）、n<=0 panic、nil 容错、特征位清 Sized/Sorted/Distinct、splitN 降级
- [x] `SummaryStats[N]`/`Summarizing[N]()`/根包 `Summary`：单遍 count/sum/min/max、Avg 派生、String 可读、Combiner 并行合并与串行等价
- [x] `RangeClosed`（闭区间、溢出拆分承接、可分保持）与 `OfNonZero`（零值过滤、comparable 约束；原 OfNonNil 随用户反馈更名——zero ⊇ nil，对齐 cmp.Or/lo.Compact 术语）
- [x] 每任务独立提交（feat×5）；质量门槛全绿：go fix 无改写 / gofmt 空 / vet 无告警 / `go test -race -count=1 ./...` 全绿 / golangci-lint 0 issues
- [x] README（Features/API Overview/Java 对照表）与 docs/api.md（构造/中间/终止/包级聚合/Collector 章节与示例）同步

## 双流条件连接 Join（Task 24）
- [x] 方法 `Join[U, R]`（InnerJoin：仅命中 `on(t,u)` 的元素对产出 `combine`）+ 方法 `LeftJoin[U, R]`（左外连接：无命中左元素以 U 零值恰好产出一条）——**修订：原「包级 Join」随用户反馈统一方法化**；不设 RightJoin（以右流作接收者调 `LeftJoin` 代替）
- [x] 求值形态：right 流 collectingSink 全量物化（不可无限）+ left 流单遍流式驱动（可无限，短路正常）；产出序左主右从（外层左流遇序、内层右流物化序）
- [x] 特征位双侧按位与清 Sized/SubSized/Sorted/Distinct；双流算子 splitN 降级（并行自动串行）；双流一次性（双侧 checkLinked）；OnClose 回调链 mergeClosers 继承
- [x] 错误即值：right 物化/left 驱动首错记入共享 evalCtx、短路、部分结果保留、`Err()` 可查；回调 panic 原样传播（全程发起 goroutine，无后台中转）
- [x] nil 容错：`on`/`combine`/`other` nil panic（对齐 Zip；原包级版 nil 返回空流容错随方法化移除）
- [x] 单测 + fuzz 等价（FuzzJoinEquivalence）；质量门槛全绿：go fix 无改写 / gofmt 空 / vet 无告警 / `go test -race -count=1 ./...` 全绿 / golangci-lint 0 issues / 覆盖率保持 100%
- [x] **SpLimited 有限性守卫**：特征位仅已知有限置位（Sized⇒Limited 不变式、FromMap 置位、FromFunc/FromSeq/FromChannel/Generate/Iterate 不置位）；透传类保留、物化类强制置位、双流双侧 AND；Join/LeftJoin 链接期右流缺 SpLimited 即 panic（fail-fast）；TestSpLimitedCharacteristics/Propagation/TestJoinFiniteGuard 覆盖
- [x] 文档同步：example_test.go、example/join/main.go（独立可运行示例）、README/README_CN（含示例清单）、docs/api.md、docs/design.md、skills/go-stream/SKILL.md

## Concat/Cache 方法化（Task 25）
- [x] 范围判定：存量包级函数逐一核对——仅 Concat/Cache 可方法化（不动 T 约束、不返回 T 派生类型）；Distinct/Contains/Sorted/Min/Max/Sum/Avg/Summary（约束 T）与 Chunk/Enumerate/WindowSliding（派生类型实例化循环）维持包级
- [x] 方法 `func (s *Stream[T]) Concat(other *Stream[T]) *Stream[T]`（nil 接收者返回 other、other nil 返回本流，语义不变）与 `func (s *Stream[T]) Cache() func() *Stream[T]`（一次性/错误记忆语义不变）
- [x] 旧包级 `Concat(a, b)`/`Cache(s)` 保留签名转一行委托 adapter：`Deprecated:` godoc + `//go:fix inline`；本版仅 deprecated，下一版本移除并标 BREAKING（inline fixer Go 1.27 仅同包内重写，跨包手工迁移已写入文档）
- [x] 仓内调用点全迁移（number_stream.go/全部测试/example basics+ lifecycle），deprecated 函数仓内零引用（SA1019 清洁）；外部模块验证旧签名行为一致
- [x] 文档同步：docs/api.md（方法形态 + 迁移说明）、docs/design.md、README/README_CN、skills/go-stream/SKILL.md；spec「形态原则与方法化迁移」Requirement
- [x] 质量门槛全绿：go fix / gofmt 空 / vet 无告警 / `go test -race -count=1 ./...` 全绿 / golangci-lint 0 issues / example 模块 build 通过

## 时间窗口重采样（Task 26；独立分支 feat/time-window 立项时自编号 Task 24，随 master 侧 Task 24/25 并入顺延）
- [x] `TimeWindow[T](s, ts, d) *Stream[TimeBucket[T]]`：`ts(v).Truncate(d)` 桶化 + GroupBy 语义（桶序=键首现序、桶内保遇序、晚到并入既有桶不拆分、不产空桶）；`TimeBucket[T]{Start, Items}` 导出类型
- [x] 桶级聚合不设独立入口（用户 amend 裁撤 TimeWindowBy）：由 `Map` 组合表达，桶内可内联聚合或经 FromSlice 子流交任意 collector
- [x] 独立新文件交付（用户 amend：不与既有实现混置）：time_window.go / time_window_test.go / time_window_example_test.go；独立内联两段式，不改 newStateful 既有签名
- [x] 包级函数形态实测论证：方法返回 Stream[TimeBucket[T]]（T 的派生类型）触发实例化循环（T instantiated as TimeBucket[T]）
- [x] 物化型 → 并行降级、不支持无限源（可先 Limit）；特征位置 SpSized/SpSubSized 清 SpSorted/SpDistinct；上游出错不产出（Err() 可查）；ts/d 非法 panic、nil 流返回 nil
- [x] 单测覆盖上述语义与 panic 矩阵（time_window.go 覆盖率 100%）；质量门槛全绿（go fix/gofmt/vet/`go test -race`/golangci-lint）；README/README_CN/docs/api.md/SKILL.md 同步
- [x] 合并 master 后增补：特征位补 SpLimited（对齐物化型统一规则；TimeWindow 输出可作 Join/LeftJoin 右流，TestTimeWindowJoinRight 守护——曾随 feat/join 并行开发缺失而误触有限性守卫 panic）
- [x] 增补（评审）：桶键 `.UTC()` 规范化——time.Time 作 map 键按结构体 ==（含 Location 指针）判等，混合时区表示的等值瞬间曾被拆成两桶；TestTimeWindowMixedLocations 守护
- [x] 增补（评审）：FuzzTimeWindowEquivalence（随机序列 + 随机窗口宽度 vs 参考 map 分桶逐桶等价 + 展平守恒）；TestTimeWindowJoinRight 作 Join/LeftJoin 右流集成回归
