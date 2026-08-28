# Checklist

## 架构与核心机制（组合替代继承）
- [ ] Stream 为具体泛型 struct（非接口），中间/终止操作通过 Go 1.27 泛型方法实现（如 `Map[U any]`、`Zip[U, R]`、`Collect[A, R]`），未在任何接口中声明带类型参数的方法
- [ ] `Stream[T]` 通过嵌入 `pipeline[T]` 组合核心求值机；算子以"构造函数 + wrap 闭包"实现，无类继承层次、无"模拟抽象类待覆写"的基类型
- [ ] Splitterator 各实现嵌入 `baseSplitterator[T]` 复用公共字段与默认 TrySplit
- [ ] 中间操作惰性：仅追加管道 stage，不触发遍历、不物化数据
- [ ] 无状态操作在终止求值时融合为单遍历（wrapSink 反向包装 + 源单遍推动）
- [ ] 有状态物化型操作在求值时先驱动上游段物化为 `[]T` 再续段（分段求值）；`Scan`/`Chunk`/`Enumerate` 单遍有状态、不物化
- [ ] 同一 Stream 实例仅可消费/链接一次，重复使用 panic 且错误信息清晰（中间操作链接上游时即检查）
- [ ] 短路语义：Limit/First/AnyMatch/TakeWhile 等能提前终止源遍历（无限源测试通过）

## 错误即值模型
- [ ] 可预期错误（`FromFunc` 源失败、`MapErr`/`FilterErr`/`FlatMapErr`/`PeekErr` 回调返回错误）以 error 值传播：首错短路、部分结果保留、`Err()` 返回首个错误
- [ ] 普通算子回调无 error 签名（纯路径零噪声，对齐 `slices` 包风格）
- [ ] 不可恢复错误 panic：重复消费、nil 回调；用户回调 panic 原样传播
- [ ] `ToMap` 键冲突 last-wins（文档明示），`ToMapMerge` 提供自定义合并

## API 完整性
- [ ] 构造函数齐全：Of/FromSlice/FromSeq/FromChannel/FromMap/FromFunc/Generate/Iterate/Range/Concat/Empty
- [ ] 无状态中间操作齐全：Filter/Map/FlatMap/FlatMapSeq/Peek/TakeWhile/DropWhile + MapErr/FilterErr/FlatMapErr/PeekErr
- [ ] 有状态中间操作齐全：Limit/Skip/Sorted(稳定)/DistinctBy/Reverse + Scan/Chunk/Enumerate + Zip
- [ ] 终止操作齐全：ForEach/ForEachUntil/ToSlice/Count/Reduce/ReduceOpt/Collect/First/FindAny/AnyMatch/AllMatch/NoneMatch/Min/Max/Err
- [ ] Collector 与预置收集器齐全：ToSlice/ToSet/ToMap/ToMapMerge/GroupingBy/Joining/Counting/Reducing/Mapping
- [ ] 包级便捷函数齐全：Contains/Sorted/Min/Max/Sum/Avg/Distinct（泛型约束补偿设计）
- [ ] Splitterator 接口含 TryAdvance/ForEachRemaining/TrySplit/EstimateSize/Characteristics，特征位齐全且沿管道正确传播
- [ ] Map 操作类型迁移静态类型安全（编译期检查）

## 并行 TODO 预留（本阶段不实现）
- [ ] TrySplit/EstimateSize/Characteristics 语义完整（slice/range 可二分）
- [ ] Collector 含 Combiner 字段；newStateful 物化闭包签名可扩展
- [ ] README 路线图明确列出 `Parallel(n)` 后续计划

## 文档（Markdown）
- [ ] README.md：简介/安装/快速上手/API 速览/与 Java 对照表/设计要点/路线图（含并行 TODO）
- [ ] docs/design.md：架构原理（管道/Sink/Splitterator/分段求值/错误模型/组合替代继承映射表）
- [ ] docs/api.md：分组 API 参考 + 示例
- [ ] example_test.go 可运行示例，与文档示例一致

## 质量验证
- [ ] `go build ./...` 通过（go 1.27，模块路径 github.com/JayceChant/go-stream）
- [ ] `go vet ./...` 无告警
- [ ] `go test ./...` 全绿（含空流、单元素、超界参数、短路计数探针、TrySplit 不重叠并集完整、稳定排序、DistinctBy 首见保留、重复消费 panic、Err 短路部分结果、FromFunc 错误、ToMap last-wins、Scan/Chunk/Zip 语义）
- [ ] benchmark 产出：Filter+Map+ToSlice 相对手写 for 循环额外开销 <3x
- [ ] 全部公开 API 具备中文 godoc 注释
