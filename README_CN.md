# go-stream

[![CI](https://github.com/JayceChant/go-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/JayceChant/go-stream/branch/master/graph/badge.svg)](https://codecov.io/gh/JayceChant/go-stream)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=JayceChant_go-stream&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=JayceChant_go-stream)
[![govulncheck](https://github.com/JayceChant/go-stream/actions/workflows/govulncheck.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/govulncheck.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/JayceChant/go-stream/badge)](https://api.scorecard.dev/projects/github.com/JayceChant/go-stream)
[![CodeQL](https://github.com/JayceChant/go-stream/actions/workflows/codeql.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/JayceChant/go-stream.svg)](https://pkg.go.dev/github.com/JayceChant/go-stream)

[English](./README.md) | 简体中文

Java Stream API 的 Go 1.27 泛型实现——基于新落地的**泛型方法**特性，第一次用原生 Go 写出自然的链式流式处理：

```go
stream.Of(1, 2, 3, 4, 5).
    Filter(func(n int) bool { return n%2 == 1 }).
    Map(func(n int) int { return n * n }).
    ToSlice() // [1 9 25]
```

Go 1.27 之前方法不能声明自有类型参数，`Map[U]` 这类链式 API 只能用包级函数（`stream.Map(s, f)`），链式手感全无；泛型方法落地后，`Stream[T]` 上的方法可以自带类型参数，实现完整的链式声明与编译期类型迁移。

## 特性

- **惰性管道**：中间操作仅声明管道不触发遍历，终止操作触发一次单遍融合求值
- **泛型方法**：`Map[U]`/`Zip[U, R]`/`Collect[A, R]` 等方法级类型参数，元素类型沿管道静态迁移
- **单遍融合**：无状态算子求值时融合为单遍（Sink 链），有状态算子分段物化
- **短路求值**：`Limit`/`First`/`AnyMatch`/`TakeWhile` 等满足条件即停止源遍历（无限流安全）
- **错误即值**：可预期错误（IO 源失败、`MapErr` 族回调错误）以 `error` 值传播——首错短路、部分结果保留、`Err()` 查询；不可恢复错误（重复消费、nil 回调）panic
- **组合替代继承**：Java 的抽象类层次（AbstractPipeline/StatelessOp/StatefulOp）转换为「结构体嵌入 + 构造函数 + 函数值注入」，无模拟继承
- **零第三方依赖**：v1 运行时无第三方依赖

## 安装

```bash
go get github.com/JayceChant/go-stream
```

要求 go 1.27+（依赖泛型方法特性）。

## 快速上手

```go
import (
    "github.com/JayceChant/go-stream"
    "github.com/JayceChant/go-stream/collector" // 收集器子包（按需）
)

// 1. 从容器构造（惰性，不触发遍历）
s := stream.FromSlice(data)          // 零拷贝引用
r := stream.Range(0, 100)            // [0, 100) 整数区间
g := stream.Generate(func() int { return 42 }) // 无限生成器

// 2. 中间操作（返回新 Stream，链式）
s.Filter(p).Map(f).Sorted(cmp).Limit(10)

// 3. 终止操作（触发一次求值，消费流）
s.ToSlice()
s.Count()
s.AnyMatch(p)
s.Collect(collector.GroupingBy(keyOf, valOf))
```

更多可运行示例：[example_test.go](./example_test.go)（`go test` 即验证）与 [example/](./example) 目录——六个独立完整、可整文件复制的示例程序，覆盖全部 API 面：

```bash
go -C example run ./basics      # 构造 → 中间 → 终止全流程
go -C example run ./collectors  # 收集器族 + 自定义 Collector（TopN）
go -C example run ./numeric     # 数值聚合、Scan 前缀和、无限源、Zip/Chunk/Enumerate
go -C example run ./errors      # 错误即值模型（FromFunc/Err 族/Err()）
go -C example run ./parallel    # 并行 Parallel(n)/Unordered、保序合并、自动降级
go -C example run ./lifecycle   # OnClose/Close 资源管理、Cache 可重放
```

`example/` 为独立 Go 模块（不参与库的测试与覆盖率统计），每个文件都可直接复制进你的项目改用。

## 实现对比

### 风格

同一个任务——筛出正数金额、降序排序、取前三、格式化为价格字符串。`Sorted` 与 `Limit` 都是有状态算子，强制管道在中途物化一次，且顺序敏感：排序必须在 `Limit` 之前（取的是排序后的前三），格式化必须在其后。手写代码别无选择，只能拆成两个循环：

**无依赖纯 Go：**

```go
// 纯 Go：一次性循环最直接，但有状态步骤会把管道
// 拆成两个循环加一次就地排序，管道结构在代码里消失。
var amounts []int
for _, n := range orders { // 循环 1：只装得下无状态的 Filter
    if n > 0 {
        amounts = append(amounts, n)
    }
}
slices.SortFunc(amounts, func(a, b int) int { return b - a }) // 物化点：必须等全部元素就位
var top []string
for i, n := range amounts { // 循环 2：取前三与格式化只能等在这里
    if i >= 3 {
        break
    }
    top = append(top, fmt.Sprintf("$%d", n))
}
```

**函数式流式操作（Go 1.27 前，无泛型方法）：**

```go
// 包级函数：类型安全、可组合，但调用层层嵌套、由内向外读——
// 数据源被埋在最里层，阅读顺序与执行顺序完全相反。
result := stream.ToSlice( // 5. 最后执行，却写在最外层
    stream.Map( // 4. 格式化前三名
        stream.Limit( // 3. 取排序结果的前三
            stream.Sorted( // 2. 降序排序，强制物化
                stream.Filter(stream.FromSlice(orders), // 1. 数据源，最先读
                    func(n int) bool { return n > 0 }),
                func(a, b int) int { return b - a },
            ),
            3,
        ),
        func(n int) string { return fmt.Sprintf("$%d", n) },
    ),
)
```

**当前实现（泛型方法）：**

```go
// 泛型方法：按管道顺序自上而下阅读，元素类型沿链迁移（int → string），
// 有状态步骤无缝插入链中。
result := stream.FromSlice(orders).
    Filter(func(n int) bool { return n > 0 }).
    Sorted(func(a, b int) int { return b - a }). // 有状态：物化后排序
    Limit(3). // 有状态：取排序结果的前三
    Map(func(n int) string { return fmt.Sprintf("$%d", n) }).
    ToSlice()
```

| 风格 | 优点 | 缺点 |
|---|---|---|
| 无依赖纯 Go | 零开销、零依赖 | 有状态步骤强制两个循环 + 就地排序；惰性、短路、错误传播、并行都要手写；阶段越多，循环体越混作一团 |
| 包级函数（1.27 前） | 类型安全、惰性、可组合 | 嵌套调用由内向外读，链式手感全无——管道越长越明显 |
| 泛型方法（当前实现） | 自上而下阅读、类型沿链迁移；有状态步骤无缝插入链中；惰性/短路/并行开箱即得 | 运行时开销——有状态算子的物化成本 + 分发开销，量化见下方「性能」 |

可读性只是故事的一半；下方「性能」小节量化这套抽象的运行时代价，供你按场景权衡取舍。

### 性能

`Filter+Map+ToSlice` 相对手写 for 循环（含 `strconv.Itoa` 的真实场景）：

| 规模 | 管道 | 手写 for | 开销倍数 |
|---|---|---|---|
| 1e2 | ~2.8 μs | ~1.0 μs | 2.8x |
| 1e4 | ~0.48 ms | ~0.18 ms | 2.6x |
| 1e6 | ~35 ms | ~22 ms | 1.6x |

目标 <3x 达标（AMD Ryzen 5 7535U，benchtime 300ms；复现：`go test -bench . -run '^$'`）。

## API 速览

| 类别 | API |
|---|---|
| 构造 | `Of` `FromSlice` `FromSeq` `FromChannel` `FromMap` `FromFunc` `Generate` `Iterate` `Range` `Concat` `Empty` |
| 无状态中间 | `Filter` `Map` `FlatMap` `FlatMapSeq` `Peek` `TakeWhile` `DropWhile` |
| Err 变体 | `MapErr` `FilterErr` `FlatMapErr` `PeekErr` |
| 有状态中间 | `Limit` `Skip` `Sorted` `DistinctBy` `Reverse` `Scan` |
| 并行控制 | `Parallel(n)` `Sequential()` `Unordered()` |
| 包级中间 | `Distinct` `Sorted`（自然序）`Chunk` `Enumerate` |
| 双流 | `Zip` |
| 生命周期 | `OnClose(f)` `Close()` `Cache(s)`（可重放工厂） |
| 终止 | `ForEach` `ForEachUntil` `ToSlice` `Count` `Reduce` `ReduceOpt` `Collect` `First` `FindAny` `AnyMatch` `AllMatch` `NoneMatch` `Min` `Max` `Err` |
| 收集器（子包 `collector`） | `ToSlice` `ToSet` `ToMap` `ToMapMerge` `GroupingBy` `Joining` `Counting` `Reducing` `Mapping` |
| 收集器（根包） | `Summing`（依赖 Number 约束） |
| 包级聚合 | `Sum` `Avg` `Contains` `Min` `Max` |

完整参考与示例见 [docs/api.md](./docs/api.md)。

## 与 Java Stream 对照

| Java | go-stream | 差异说明 |
|---|---|---|
| `Stream<T>`（接口） | `*Stream[T]`（具体 struct） | Go 1.27 接口方法不能声明类型参数，泛型方法必须挂在具体类型上 |
| `stream.of(...)` / `Arrays.stream` | `stream.Of(...)` / `stream.FromSlice` | |
| `Collectors.toList()` | `collector.ToSlice[T]()` | |
| `Collectors.toMap` | `collector.ToMap` / `ToMapMerge` | 键冲突 last-wins（对齐 Go map 惯例），自定义合并用 ToMapMerge |
| `Collectors.groupingBy` | `collector.GroupingBy` | 组内保遇序 |
| `Comparator` | `func(a, b T) int` | 对齐标准库 `slices.SortFunc`/`cmp.Compare` 惯例 |
| `IntStream` 特化族 | 泛型 + `Number`/`cmp.Ordered` 约束 | Go 泛型零装箱，无需特化 |
| `stream.parallel()` | `Parallel(n)` / `Sequential()` | TrySplit 分片 + goroutine；短路终止与物化算子后自动降级串行 |
| `stream.unordered()` | `Unordered()` | 清除 SpOrdered；并行下分片结果先完成先推（流式合并） |
| `stream.onClose(f)` / `close()` | `OnClose(f)` / `Close()` | 求值结束（含短路/错误/panic 路径）自动触发；显式关闭幂等；回调出错经 `Err()` 查询 |
| 异常穿透 | 错误即值（`Err()`/`MapErr` 族） | 对齐 Go 官方错误风格 |
| `stream.distinct()` | `DistinctBy[K comparable](key)` 方法 / `Distinct` 包级 | 方法自有类型参数可带 `comparable` 约束（键类型编译期可比较、零装箱）；`Distinct` 约束的是元素 `T` 本身，方法不能约束接收者的 `T`，故仍为包级 |

## 设计要点

- **Sink 推送链**：求值时从终止操作出发反向包装 Sink（`Accept(t) bool` 返回值融合 Java 的 `cancellationRequested`），数据源单遍推动元素流过整条链
- **分段求值**：`Sorted`/`Skip` 等有状态算子先驱动上游物化为 `[]T` 再变换回放；`Limit` 支持无限源短路收集；`Skip(0)` 恒等返回原流（真正 no-op：不物化、特征位透传）
- **特征位传播**：`SpSized`/`SpOrdered`/`SpSorted`/`SpDistinct` 沿管道传播（如 Map 1:1 保留 Sized 使下游预分配），为并行拆分提供决策依据
- **错误模型**：参照 `bufio.Scanner.Err()` 惯例——出错时终止操作返回已累积的部分结果，`Err()` 返回首错

架构详情见 [docs/design.md](./docs/design.md)。

## 路线图

- [x] v1 串行求值引擎、全量算子、Collector 体系、错误即值模型
- [x] **并行求值 `Parallel(n)` / `Sequential()`**：TrySplit 递归分片 + goroutine 并行执行 + Collector.Combiner 合并；按分片序合并保序（Ordered）、短路终止族与物化算子后自动降级串行（正确性优先）；CPU 密集场景实测加速比 ~3.3x（4 分片）
- [x] v1.x：**`onClose`/资源管理**（`OnClose(f)` 求值结束自动触发 + `Close()` 幂等显式释放）、**可重放流**（`Cache(s)` 工厂：物化一次、每次产全新一次性流，不破坏一次性模型）、**Unordered 流式合并**（`Unordered()` 清序标志，并行下分片先完成先推，降低端到端延迟）

## License

MIT
