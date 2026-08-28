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
| `Range[I Integer](start, stop I) *Stream[I]` | 左闭右开整数区间（可 TrySplit） |
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
| `Peek(f func(T)) *Stream[T]` | 副作用观察（调试） |
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
| `Limit(n int) *Stream[T]` *短路* | 截取前 n（无限源安全） |
| `Skip(n int) *Stream[T]` | 跳过前 n |
| `Sorted(cmp func(a, b T) int) *Stream[T]` | 稳定排序（对齐 `slices.SortFunc`） |
| `DistinctBy(key func(T) any) *Stream[T]` | 按键去重（保留首见；键须可比较） |
| `Reverse() *Stream[T]` | 反转 |

有状态（单遍型，不物化）：

| 方法 | 说明 |
|---|---|
| `Scan[U](seed U, f func(U, T) U) *Stream[U]` | 滚动累积（前缀和；含初值共 n+1 项） |

包级中间操作（方法无法追加元素类型约束或受实例化循环限制）：

| 函数 | 说明 |
|---|---|
| `Distinct[T comparable](s) *Stream[T]` | 天然去重（保留首见） |
| `Sorted[T cmp.Ordered](s) *Stream[T]` | 自然序排序 |
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

```go
// 并行求值：结果与串行一致（按分片序合并）
got := stream.FromSlice(bigData).
    Parallel(runtime.NumCPU()).
    Filter(expensive).
    Map(heavy).
    ToSlice()
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

包级聚合（约束补偿设计）：

| 函数 | 说明 |
|---|---|
| `Sum[N Number](s *Stream[N]) N` | 求和 |
| `Avg[N Number](s *Stream[N]) N` | 平均（空流 0） |
| `Contains[T comparable](s, target T) bool` *短路* | 包含判断 |
| `Min[T cmp.Ordered](s) (T, bool)` / `Max[T cmp.Ordered](s)` | 自然序最值 |

## Collector

```go
type Collector[T, A, R any] struct {
    Supplier    func() A              // 创建累积容器（建议指针类型）
    Accumulator func(A, T)            // 累积单元素
    Combiner    func(A, A) A          // 合并两容器（并行分片预留）
    Finisher    func(A) R             // 最终变换
}
```

| 预置收集器 | 说明 |
|---|---|
| `ToSlice[T]()` | 切片 |
| `ToSet[T]()` | `map[T]struct{}` |
| `ToMap[K, V, T](keyF, valF)` | map；键冲突 last-wins |
| `ToMapMerge[K, V, T](keyF, valF, merge)` | map；键冲突以 merge(old, new) 合并 |
| `GroupingBy[K, V, T](keyF, valF)` | `map[K][]V` 分组，组内保遇序 |
| `Joining[T](strF, sep)` | 字符串拼接 |
| `Counting[T]()` | 计数 |
| `Reducing[T](identity, op)` | 折叠 |
| `Mapping[T, U, A, R](f, downstream)` | 先变换再汇聚（组合子） |
| `Summing[N Number]()` | 数值求和 |

```go
// Mapping 组合子：分组后求每组的和
sums := stream.FromSlice(orders).Collect(stream.GroupingBy(
    func(o Order) string { return o.Region },
    func(o Order) int { return o.Amount },
))

// ToMapMerge：同键金额累加
total := stream.FromSlice(orders).Collect(stream.ToMapMerge(
    func(o Order) string { return o.Region },
    func(o Order) int { return o.Amount },
    func(old, new int) int { return old + new },
))
```

## 语义约定

- **一次性消费**：同一 Stream 仅可链接或消费一次，重复使用 panic
- **惰性**：中间操作不触发遍历；无终止操作则零成本
- **错误即值**：出错时终止操作返回已累积的部分结果；`Err()` 返回首错
- **比较器**：统一 `func(a, b T) int`（负/零/正），对齐 `slices.SortFunc`/`cmp.Compare`
