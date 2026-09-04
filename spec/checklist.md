# Checklist

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
- [x] Collector 与预置收集器齐全：ToSlice/ToSet/ToMap/ToMapMerge/GroupingBy/Joining/Counting/Reducing/Mapping（另有 Summing）；已迁移至低耦合子包 `collector`（零依赖叶子包），Summing 因依赖 Number 约束留根包
- [x] 包级便捷函数齐全：Contains/Sorted/Min/Max/Sum/Avg/Distinct（泛型约束补偿设计）
- [x] Splitterator 接口含 TryAdvance/ForEachRemaining/TrySplit/EstimateSize/Characteristics，特征位齐全且沿管道正确传播
- [x] Map 操作类型迁移静态类型安全（编译期检查）

## 并行（Task 8 已实现）
- [x] TrySplit/EstimateSize/Characteristics 语义完整（slice/range 可二分，前后不重叠并集完整）
- [x] Collector 含 Combiner 字段且全部预置收集器实现分片合并；newStateful 物化闭包签名已用于降级判断
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
