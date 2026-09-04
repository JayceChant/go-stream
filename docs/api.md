# API 参考

> 包 `stream`（`github.com/JayceChant/go-stream`）。全部操作按「构造 → 中间 → 终止」分节；标注 *短路* 的操作满足条件即停止源遍历。
>
> 更多可运行示例见 [example_test.go](../example_test.go)。

## 构造函数（源适配）

全部惰性：构造不触发遍历。

| 函数 | 说明 |
|---|---|
| `Of[T](xs ...T) *Stream[T]` | 少量元素直构 |
| `FromSlice[T](s []T) *Stream[T]` | 零拷贝引用原切片（Sized/Ordered，可 TrySplit） |
| `Empty[T]() *Stream[T]` | 空流 |
| `FromSeq[T](seq iter.Seq[T]) *Stream[T]` | 适配 Go 1.23 range-over-func 迭代器 |
| `FromChannel[T](ch <-chan T) *Stream[T]` | channel 源，接收至关闭 |
| `FromMap[K, V](m map[K]V) *Stream[KV[K,V]]` | map 源，产出 `KV` 键值对（Unordered） |
| `FromFunc[T](next func() (T, bool, error)) *Stream[T]` | 拉式源（IO/解析场景），错误即值入口 |
| `Generate[T](f func() T) *Stream[T]` | 无限生成器 |
| `Iterate[T](seed T, next func(T) T) *Stream[T]` | 无限迭代（seed, f(seed), ...） |
| `Range[I Integer](start, stop I) *NumberStream[I]` | 左闭右开整数区间（可 TrySplit）；直接返回数值流（Task 18），需 `*Stream` 时经 `AsStream()` 桥接 |
| `Concat[T](a, b *Stream[T]) *Stream[T]` | 顺序拼接两流 |

```go
// FromFunc：第 4 次读取出错，ToSlice 保留前 3 个，Err() 返回首错
s := stream.FromFunc(func() (int, bool, error) { ... })
got := s.ToSlice() // [1 2 3]
err := s.Err()
```

## 中间操作

无状态（求值时单遍融合）：

| 方法 | 说明 |
|---|---|
| `Filter(p func(T) bool) *Stream[T]` | 保留满足 p 的元素 |
| `Map[U](f func(T) U) *Stream[U]` | 1:1 变换（泛型方法，类型迁移） |
| `FlatMap[U](f func(T) []U) *Stream[U]` | 展开子切片 |
| `FlatMapSeq[U](f func(T) iter.Seq[U]) *Stream[U]` | 惰性子序列展开 |
| `Peek(f func(T)) *Stream[T]` | 副作用观察（调试）；并行流下 f 在分片 goroutine 内执行，顺序不保证 |
| `TakeWhile(p) *Stream[T]` *短路* | 保留首批满足 p 的元素 |
| `DropWhile(p) *Stream[T]` | 丢弃首批满足 p 的元素 |

Err 变体（错误即值：回调返回错误 → 首错短路、部分结果保留、`Err()` 查询）：

| 方法 | 说明 |
|---|---|
| `MapErr[U](f func(T) (U, error)) *Stream[U]` | 可失败的 1:1 变换 |
| `FilterErr(p func(T) (bool, error)) *Stream[T]` | 可失败过滤 |
| `FlatMapErr[U](f func(T) ([]U, error)) *Stream[U]` | 可失败展开 |
| `PeekErr(f func(T) error) *Stream[T]` | 可失败观察 |

有状态（物化型，分段求值）：

| 方法 | 说明 |
|---|---|
| `Limit(n int64) *Stream[T]` *短路* | 截取前 n（n==0 空流；无限源安全；n<0 panic） |
| `Skip(n int64) *Stream[T]` | 跳过前 n（n==0 恒等返回原流，不物化、特征位透传；n<0 panic） |
| `Sorted(cmp func(a, b T) int) *Stream[T]` | 稳定排序（对齐 `slices.SortFunc`） |
| `DistinctBy[K comparable](key func(T) K) *Stream[T]` | 按键去重（保留首见；键类型编译期须可比较，免装箱；K 取 `any` 时动态类型不可比较仍在求值时 panic） |
| `Reverse() *Stream[T]` | 反转 |

有状态（单遍型，不物化）：

| 方法 | 说明 |
|---|---|
| `Scan[U](seed U, f func(U, T) U) *Stream[U]` | 滚动累积（前缀和；含初值共 n+1 项） |

包级中间操作（方法无法追加元素类型约束或受实例化循环限制）：

| 函数 | 说明 |
|---|---|
| `Distinct[T comparable](s) *Stream[T]` | 天然去重（保留首见） |
| `Sorted[T cmp.Ordered](s) *Stream[T]` | 自然序排序（不稳定；稳定形态用 `s.StableSorted(cmp.Compare[T])`） |
| `Chunk[T](s, n int) *Stream[[]T]` | 定长分组（尾组可不足 n） |
| `Enumerate[T](s) *Stream[KV[int, T]]` | 附加索引（对应 `for i, v := range`） |

双流：

| 方法 | 说明 |
|---|---|
| `Zip[U, R](other *Stream[U], f func(T, U) R) *Stream[R]` | 按位置配对，取短；两条流均被消费 |

并行控制：

| 方法 | 说明 |
|---|---|
| `Parallel(n int) *Stream[T]` | 声明后续求值最多 n 个分片并行（TrySplit 分片 + goroutine；短路终止与物化算子后自动降级串行） |
| `Sequential() *Stream[T]` | 还原串行求值 |
| `Unordered() *Stream[T]` | 声明不依赖相遇顺序（清 SpOrdered；并行求值下分片结果先完成先推，降低端到端延迟） |

```go
// 并行求值：结果与串行一致（按分片序合并）
got := stream.FromSlice(bigData).
    Parallel(runtime.NumCPU()).
    Filter(expensive).
    Map(heavy).
    ToSlice()
```

生命周期与可重放（Task 10）：

| 方法/函数 | 说明 |
|---|---|
| `OnClose(f func() error) *Stream[T]` | 注册清理回调：终止求值结束自动触发（耗尽/短路/错误/panic 路径均触发）；按注册序执行，出错记首错经 `Err()` 查询 |
| `Close() error` | 显式关闭（幂等；未求值流也可关闭）；返回回调链首错 |
| `Cache[T](s *Stream[T]) func() *Stream[T]` | 可重放工厂：首次调用求值上游一次并物化，此后每次返回全新一次性流（FromSlice 零拷贝）；物化期首错记忆，此后返回携带错误的空流 |

```go
// 求值结束自动释放资源
ch := make(chan int, 3)
stream.FromChannel(ch).OnClose(func() error { return file.Close() }).ToSlice()

// 无序流式合并：快分片完成即推入终端，不等慢分片
set := stream.FromSlice(bigData).Parallel(4).Unordered().
    Collect(collector.ToSet[int]())

// 可重放：上游只求值一次
f := stream.Cache(expensiveQuery())
f().ForEach(use)   // 首次：物化
f().ForEach(use)   // 重放：零拷贝
```

## 终止操作

| 方法 | 说明 |
|---|---|
| `ForEach(f func(T))` | 逐元素执行 |
| `ForEachUntil(f func(T) bool)` | f 返回 false 提前终止 |
| `ToSlice() []T` | 收集为新切片 |
| `Count() int64` | 计数 |
| `Reduce(identity T, op func(T, T) T) T` | 有初值折叠 |
| `ReduceOpt(op func(T, T) T) (T, bool)` | 无初值折叠（空流零值+false） |
| `Collect[A, R](c Collector[T, A, R]) R` | 收集器汇聚（泛型方法） |
| `First() (T, bool)` *短路* | 首元素 |
| `FindAny(p func(T) bool) (T, bool)` *短路* | 顺序流下等价 Filter+First |
| `AnyMatch(p func(T) bool) bool` *短路* | 存在满足 |
| `AllMatch(p func(T) bool) bool` *短路* | 全部满足（空流 true） |
| `NoneMatch(p func(T) bool) bool` *短路* | 无满足（空流 true） |
| `Min(cmp func(a, b T) int) (T, bool)` | 最小（空流零值+false） |
| `Max(cmp func(a, b T) int) (T, bool)` | 最大 |
| `Err() error` | 最近一次求值的首错（错误即值） |

包级聚合（约束补偿设计；链式方法形态见下节 NumberStream）：

| 函数 | 说明 |
|---|---|
| `Sum[N Number](s *Stream[N]) N` | 求和 |
| `Avg[N Number](s *Stream[N]) N` | 平均（空流 0） |
| `Contains[T comparable](s, target T) bool` *短路* | 包含判断 |
| `Min[T cmp.Ordered](s) (T, bool)` / `Max[T cmp.Ordered](s)` | 自然序最值 |
| `Distinct[T comparable](s) *Stream[T]` | 元素自身去重（保首见遇序） |

## NumberStream（数值流，Task 18）

对标 Java IntStream 的**约束收窄**形态（Go 泛型零装箱，非装箱规避）：`NumberStream[N Number]` 值嵌入 `Stream[N]`，把元素约束收窄到包装类型自有类型参数位，使依赖元素约束的 API 回归**可链式调用的方法形态**；普通 `*Stream` 的包级函数继续可用（并存≠重复）。

收窄入口（返回 `*NumberStream`）：

| 入口 | 说明 |
|---|---|
| `Range[I](start, stop I)` | 区间元素必然是数值：直接返回数值流 |
| `OfNumber[N](xs ...N)` / `FromNumberSlice[N](s []N)` | `Of`/`FromSlice` 收窄版 |
| `(*Stream[T]).MapToNumber[N](f func(T) N)` | 类型迁移入窄流（对应 Java `mapToInt`） |
| `AsNumber[N](s *Stream[N])` | 通用桥接（复制句柄 + 立即标记原流消费，二次桥接 panic；nil 容错） |

核心方法（19 个）：

| 分组 | 方法 |
|---|---|
| 元素保持中间（返回 `*NumberStream[N]`，链不断窄） | `Filter` `Peek` `TakeWhile` `DropWhile` `Limit` `Skip`（`n==0` 恒等返回自身）`Reverse` |
| 自然序收窄 | `Sorted()` `StableSorted()`（免比较器）`Distinct()`（按元素值；浮点 NaN 各自保留） |
| 标志/生命周期 | `Parallel(n)` `Sequential()` `Unordered()` `OnClose(f)` |
| 收窄终端 | `Sum()` `Avg()` `Min()` `Max()` `Contains(target)` |

逃逸规则：未被覆写的提升方法保持 Stream 语义——类型迁移（`Map[U]`/`FlatMap` 族/`Scan`/`Zip`）自然返回 `*Stream`；值终端（`ToSlice`/`Count`/`First`/`Collect`/`Err` 等）直接可用；被自然序版遮蔽的比较器形态（`Sorted(cmp)`/`StableSorted(cmp)`/`Min(cmp)`/`Max(cmp)`）经显式出口 `AsStream()` 使用（消费本流，返回独立句柄）。

性能注记：每级元素保持算子与收窄入口相对等价 Stream 版多一次句柄分配（构造期一次性，实测深度 4 纯构造链 +5 allocs/+560B/+~300ns；n=100 约 +10%；n≥1e6 持平），求值热路径零差异（`BenchmarkNumberStreamVsStream`）。重构造轻求值的极端场景（每请求重建短链且元素极少）可先以 `*Stream` 串联中间操作、末步 `AsNumber` 收窄后仅接终端。

```go
// 数值链一行闭环：Range 直接收窄，Sum/Avg/Min/Max/Contains 全程方法形态
total := stream.Range(1, 101).Sum()                       // 5050
avg := stream.Range(1, 4).Avg()                            // 2（整除）
evenSum := stream.Range(0, 100).Filter(func(v int) bool {
    return v%2 == 0
}).Sum()                                                   // 2450
sorted := stream.OfNumber(3, 1, 2).Distinct().Sorted().ToSlice() // [1 2 3]

// 类型迁移入窄流（mapToInt 风格）与双向桥接
totalLen := stream.FromSlice(words).MapToNumber(func(s string) int {
    return len(s)
}).Sum()
pairs := stream.Of("a", "b").Zip(stream.Range(1, 10).AsStream(), f).ToSlice()
```

## Collector（子包 `stream/collector`）

收集器族位于低耦合子包 `collector`（`github.com/JayceChant/go-stream/collector`，无三方依赖，仅共享类型约束）。

```go
import "github.com/JayceChant/go-stream/collector"

// Collector 为接口，各收集器以非导出具体类型实现（行为只读，
// 防止外部意外改写内部逻辑）；自定义收集器实现同一边即可。
type Collector[T, A, R any] interface {
    Supplier() A                    // 创建累积容器（建议指针类型）
    Accumulator(a A, v T)           // 累积单元素
    Combiner() func(a, b A) A       // 返回合并函数；nil = 不支持并行（自动降级串行）
    Finisher(a A) R                 // 最终变换
}
```

| 预置收集器 | 说明 |
|---|---|
| `collector.ToSlice[T]()` | 切片 |
| `collector.ToSet[T]()` | `map[T]struct{}` |
| `collector.ToMap[K, V, T](keyF, valF)` | map；键冲突 last-wins |
| `collector.ToMapMerge[K, V, T](keyF, valF, merge)` | map；键冲突以 merge(old, new) 合并 |
| `collector.GroupingBy[K, V, T](keyF, valF)` | `map[K][]V` 分组，组内保遇序 |
| `collector.Joining[T](strF, sep)` | 字符串拼接 |
| `collector.Counting[T]()` | 计数 |
| `collector.Reducing[T](identity, op)` | 折叠 |
| `collector.Mapping[T, U, A, R](f, downstream)` | 先变换再汇聚（组合子） |
| `collector.Summing[N constraints.Number]()` | 数值求和 |
| `collector.Averaging[N constraints.Number]()` | 数值平均（和 + 计数同行累积，单遍；空流 0；整型整除） |

```go
// Mapping 组合子：分组后求每组的和
sums := stream.FromSlice(orders).Collect(collector.GroupingBy(
    func(o Order) string { return o.Region },
    func(o Order) int { return o.Amount },
))

// ToMapMerge：同键金额累加
total := stream.FromSlice(orders).Collect(collector.ToMapMerge(
    func(o Order) string { return o.Region },
    func(o Order) int { return o.Amount },
    func(oldV, newV int) int { return oldV + newV },
))
```

## 语义约定

- **一次性消费**：同一 Stream 仅可链接或消费一次，重复使用 panic
- **惰性**：中间操作不触发遍历；无终止操作则零成本
- **错误即值**：出错时终止操作返回已累积的部分结果；`Err()` 返回首错
- **比较器**：统一 `func(a, b T) int`（负/零/正），对齐 `slices.SortFunc`/`cmp.Compare`
