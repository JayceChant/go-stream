// collector_combine.go：Collector 组合生态（Task 20，对齐 Java 9/12 Collectors 增补）。
//
// GroupingByDownstream/PartitioningBy 对应 groupingBy(classifier, downstream)/
// partitioningBy；Teeing 对应 teeing（一次遍历双下游）；Filtering/FlatMapping
// 对应 filtering/flatMapping 下游收集器；CollectingAndThen 对应
// collectingAndThen（finisher 包装）；MinBy/MaxBy 对应 minBy/maxBy。
// 组合原则与既有收集器一致：A 为指针累积容器；Combiner 返回 nil 表示
// 不支持并行合并（Collect 自动降级串行）。

package collector

import (
	"fmt"

	"github.com/JayceChant/go-stream/constraints"
)

// groupDownCollector 是 GroupingByDownstream 的实现：每键独立下游累积容器，
// Finisher 时逐组 Finisher 收口。
type groupDownCollector[K comparable, T, A, R any] struct {
	keyF       func(T) K
	downstream Collector[T, A, R]
}

// 编译期检查：groupDownCollector 实现 Collector。
var _ Collector[int, *map[string]*int64, map[string]int64] = groupDownCollector[string, int, *int64, int64]{}

func (c groupDownCollector[K, T, A, R]) Supplier() *map[K]A {
	m := make(map[K]A)
	return &m
}

func (c groupDownCollector[K, T, A, R]) Accumulator(m *map[K]A, v T) {
	k := c.keyF(v)
	a, ok := (*m)[k]
	if !ok {
		a = c.downstream.Supplier()
		(*m)[k] = a
	}
	c.downstream.Accumulator(a, v)
}

// Combiner 支持并行合并：键冲突时以下游 Combiner 合并组内累积容器；
// 下游不支持并行（Combiner 为 nil）时整体返回 nil（降级串行）。
func (c groupDownCollector[K, T, A, R]) Combiner() func(a, b *map[K]A) *map[K]A {
	com := c.downstream.Combiner()
	if com == nil {
		return nil
	}
	return func(a, b *map[K]A) *map[K]A {
		for k, ba := range *b {
			if aa, ok := (*a)[k]; ok {
				(*a)[k] = com(aa, ba)
			} else {
				(*a)[k] = ba
			}
		}
		return a
	}
}

func (c groupDownCollector[K, T, A, R]) Finisher(m *map[K]A) map[K]R {
	out := make(map[K]R, len(*m))
	for k, a := range *m {
		out[k] = c.downstream.Finisher(a)
	}
	return out
}

// GroupingByDownstream 按 keyF 分组，每组元素交 downstream 汇聚，产出
// map[K]R（对应 Java groupingBy(classifier, downstream)，如分组计数/求和）。
// 组内保持遇序；下游 Combiner 可用时支持并行合并。
func GroupingByDownstream[K comparable, T, A, R any](keyF func(T) K, downstream Collector[T, A, R]) Collector[T, *map[K]A, map[K]R] {
	return groupDownCollector[K, T, A, R]{keyF: keyF, downstream: downstream}
}

// Partition 是 PartitioningBy 的结果：谓词真/假两组的下游收集结果。
// False 侧恒非 nil（空组为 downstream 的零值结果，如 ToSlice 的 nil 切片）。
type Partition[T, R any] struct {
	True  R
	False R
}

// partCollector 是 PartitioningBy 的实现：布尔分组，每侧独立下游累积。
type partCollector[T, A, R any] struct {
	p          func(T) bool
	downstream Collector[T, A, R]
}

// 编译期检查：partCollector 实现 Collector。
var _ Collector[int, *Partition[int, *[]int], Partition[int, []int]] = partCollector[int, *[]int, []int]{}

func (c partCollector[T, A, R]) Supplier() *Partition[T, A] {
	return &Partition[T, A]{True: c.downstream.Supplier(), False: c.downstream.Supplier()}
}

func (c partCollector[T, A, R]) Accumulator(p *Partition[T, A], v T) {
	if c.p(v) {
		c.downstream.Accumulator(p.True, v)
	} else {
		c.downstream.Accumulator(p.False, v)
	}
}

// Combiner 双侧独立合并（下游 Combiner 为 nil 时整体降级串行）。
func (c partCollector[T, A, R]) Combiner() func(a, b *Partition[T, A]) *Partition[T, A] {
	com := c.downstream.Combiner()
	if com == nil {
		return nil
	}
	return func(a, b *Partition[T, A]) *Partition[T, A] {
		a.True = com(a.True, b.True)
		a.False = com(a.False, b.False)
		return a
	}
}

func (c partCollector[T, A, R]) Finisher(p *Partition[T, A]) Partition[T, R] {
	return Partition[T, R]{True: c.downstream.Finisher(p.True), False: c.downstream.Finisher(p.False)}
}

// PartitioningBy 按谓词 p 布尔分组，两侧各自交 downstream 汇聚
// （对应 Java partitioningBy(predicate, downstream)）。
func PartitioningBy[T, A, R any](p func(T) bool, downstream Collector[T, A, R]) Collector[T, *Partition[T, A], Partition[T, R]] {
	return partCollector[T, A, R]{p: p, downstream: downstream}
}

// PartitioningBySlice 是 PartitioningBy + ToSlice 的便捷形态：
// 产出 Partition[T, []T]（两侧各为切片，空侧为 nil）。
func PartitioningBySlice[T any](p func(T) bool) Collector[T, *Partition[T, *[]T], Partition[T, []T]] {
	return PartitioningBy(p, ToSlice[T]())
}

// teeAcc 是 Teeing 的累积容器：双下游累积器同行累积。
type teeAcc[A1, A2 any] struct {
	a1 A1
	a2 A2
}

// teeCollector 是 Teeing 的实现。
type teeCollector[T, A1, R1, A2, R2, R any] struct {
	c1    Collector[T, A1, R1]
	c2    Collector[T, A2, R2]
	merge func(R1, R2) R
}

// 编译期检查：teeCollector 实现 Collector。
var _ Collector[int, *teeAcc[*int64, *[]int], string] = teeCollector[int, *int64, int64, *[]int, []int, string]{}

func (c teeCollector[T, A1, R1, A2, R2, R]) Supplier() *teeAcc[A1, A2] {
	return &teeAcc[A1, A2]{a1: c.c1.Supplier(), a2: c.c2.Supplier()}
}

func (c teeCollector[T, A1, R1, A2, R2, R]) Accumulator(acc *teeAcc[A1, A2], v T) {
	c.c1.Accumulator(acc.a1, v)
	c.c2.Accumulator(acc.a2, v)
}

// Combiner 双侧可并行时才支持（任一下游为 nil 则整体 nil，降级串行）。
func (c teeCollector[T, A1, R1, A2, R2, R]) Combiner() func(a, b *teeAcc[A1, A2]) *teeAcc[A1, A2] {
	com1, com2 := c.c1.Combiner(), c.c2.Combiner()
	if com1 == nil || com2 == nil {
		return nil
	}
	return func(a, b *teeAcc[A1, A2]) *teeAcc[A1, A2] {
		a.a1 = com1(a.a1, b.a1)
		a.a2 = com2(a.a2, b.a2)
		return a
	}
}

func (c teeCollector[T, A1, R1, A2, R2, R]) Finisher(acc *teeAcc[A1, A2]) R {
	return c.merge(c.c1.Finisher(acc.a1), c.c2.Finisher(acc.a2))
}

// Teeing 一次遍历同时喂两个下游收集器，结束后以 merge 合并双结果
// （对应 Java 12 teeing）。源只被遍历一次。
func Teeing[T, A1, R1, A2, R2, R any](c1 Collector[T, A1, R1], c2 Collector[T, A2, R2], merge func(R1, R2) R) Collector[T, *teeAcc[A1, A2], R] {
	return teeCollector[T, A1, R1, A2, R2, R]{c1: c1, c2: c2, merge: merge}
}

// filterCollector 是 Filtering 的实现：转发 Supplier/Combiner/Finisher
// 至下游，仅 Accumulator 先过谓词。
type filterCollector[T, A, R any] struct {
	p          func(T) bool
	downstream Collector[T, A, R]
}

// 编译期检查：filterCollector 实现 Collector。
var _ Collector[int, *[]int, []int] = filterCollector[int, *[]int, []int]{}

func (f filterCollector[T, A, R]) Supplier() A { return f.downstream.Supplier() }
func (f filterCollector[T, A, R]) Accumulator(a A, v T) {
	if f.p(v) {
		f.downstream.Accumulator(a, v)
	}
}
func (f filterCollector[T, A, R]) Combiner() func(A, A) A { return f.downstream.Combiner() }
func (f filterCollector[T, A, R]) Finisher(a A) R         { return f.downstream.Finisher(a) }

// Filtering 元素先过谓词 p 再交下游汇聚（对应 Java 9 filtering；
// 与流上 Filter 的区别：作为下游收集器嵌入 GroupingBy 等组合时，
// 分组内过滤不影响其它组的元素流）。
func Filtering[T, A, R any](p func(T) bool, downstream Collector[T, A, R]) Collector[T, A, R] {
	return filterCollector[T, A, R]{p: p, downstream: downstream}
}

// flatMapCollector 是 FlatMapping 的实现：元素先 1:N 展开再交下游。
type flatMapCollector[T, U, A, R any] struct {
	f          func(T) []U
	downstream Collector[U, A, R]
}

// 编译期检查：flatMapCollector 实现 Collector。
var _ Collector[int, *[]string, []string] = flatMapCollector[int, string, *[]string, []string]{}

func (m flatMapCollector[T, U, A, R]) Supplier() A { return m.downstream.Supplier() }
func (m flatMapCollector[T, U, A, R]) Accumulator(a A, v T) {
	for _, u := range m.f(v) {
		m.downstream.Accumulator(a, u)
	}
}
func (m flatMapCollector[T, U, A, R]) Combiner() func(A, A) A { return m.downstream.Combiner() }
func (m flatMapCollector[T, U, A, R]) Finisher(a A) R         { return m.downstream.Finisher(a) }

// FlatMapping 元素先经 f 1:N 展开为切片，再逐个交下游汇聚
// （对应 Java 9 flatMapping；下游元素类型随展开函数迁移）。
func FlatMapping[T, U, A, R any](f func(T) []U, downstream Collector[U, A, R]) Collector[T, A, R] {
	return flatMapCollector[T, U, A, R]{f: f, downstream: downstream}
}

// andThenCollector 是 CollectingAndThen 的实现：finisher 包装下游结果。
type andThenCollector[T, A, R, RR any] struct {
	c      Collector[T, A, R]
	finish func(R) RR
}

// 编译期检查：andThenCollector 实现 Collector。
var _ Collector[int, *[]int, int] = andThenCollector[int, *[]int, []int, int]{}

func (a andThenCollector[T, A, R, RR]) Supplier() A { return a.c.Supplier() }
func (a andThenCollector[T, A, R, RR]) Accumulator(acc A, v T) {
	a.c.Accumulator(acc, v)
}
func (a andThenCollector[T, A, R, RR]) Combiner() func(A, A) A { return a.c.Combiner() }
func (a andThenCollector[T, A, R, RR]) Finisher(acc A) RR      { return a.finish(a.c.Finisher(acc)) }

// CollectingAndThen 在下游收集结果上再施加 finish 变换
// （对应 Java collectingAndThen，如收集后取 len、转不可变视图等）。
func CollectingAndThen[T, A, R, RR any](c Collector[T, A, R], finish func(R) RR) Collector[T, A, RR] {
	return andThenCollector[T, A, R, RR]{c: c, finish: finish}
}

// minmaxAcc 是 MinBy/MaxBy 的累积容器：当前最值与是否已有元素。
type minmaxAcc[T any] struct {
	best  T
	found bool
}

// minmaxCollector 是 MinBy/MaxBy 的实现：sign 为 -1 取最小、+1 取最大。
type minmaxCollector[T any] struct {
	cmp  func(a, b T) int
	sign int
}

// 编译期检查：minmaxCollector 实现 Collector。
var _ Collector[int, *minmaxAcc[int], int] = minmaxCollector[int]{}

func (m minmaxCollector[T]) Supplier() *minmaxAcc[T] { return new(minmaxAcc[T]) }
func (m minmaxCollector[T]) Accumulator(a *minmaxAcc[T], v T) {
	if !a.found {
		a.best, a.found = v, true
		return
	}
	if m.cmp(v, a.best)*m.sign > 0 {
		a.best = v
	}
}

// Combiner 合并两片最值（空片让位）。
func (m minmaxCollector[T]) Combiner() func(a, b *minmaxAcc[T]) *minmaxAcc[T] {
	return func(a, b *minmaxAcc[T]) *minmaxAcc[T] {
		switch {
		case !b.found:
			return a
		case !a.found:
			return b
		case m.cmp(b.best, a.best)*m.sign > 0:
			a.best = b.best
		}
		return a
	}
}

func (m minmaxCollector[T]) Finisher(a *minmaxAcc[T]) T { return a.best }

// MinBy 收集器形态的最小值（依 cmp；空流返回零值，与终端 Min 的 (T, bool)
// 形态区分——收集器语境无"空"信号位，对应 Java minBy）。
func MinBy[T any](cmp func(a, b T) int) Collector[T, *minmaxAcc[T], T] {
	return minmaxCollector[T]{cmp: cmp, sign: -1}
}

// MaxBy 收集器形态的最大值（依 cmp；空流返回零值，对应 Java maxBy）。
func MaxBy[T any](cmp func(a, b T) int) Collector[T, *minmaxAcc[T], T] {
	return minmaxCollector[T]{cmp: cmp, sign: 1}
}

// SummaryStats 是单遍数值统计结果：一次遍历同时累积计数、总和、最小、最大
// （对应 Java IntSummaryStatistics/DoubleSummaryStatistics 的泛型合并形态——
// Go 泛型单结构覆盖全部数值类型）。空流时 Count=0、Sum=0、Min/Max 为零值。
type SummaryStats[N constraints.Number] struct {
	Count int64
	Sum   N
	Min   N
	Max   N
}

// Avg 返回平均值（空流返回 0；整数类型按整除语义截断，与 Averaging 一致）。
func (s SummaryStats[N]) Avg() N {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / N(s.Count)
}

// String 便于打印（"count=4, sum=10, min=1, max=4, avg=2"）。
func (s SummaryStats[N]) String() string {
	return fmt.Sprintf("count=%d, sum=%v, min=%v, max=%v, avg=%v",
		s.Count, s.Sum, s.Min, s.Max, s.Avg())
}

// summarizeCollector 是 Summarizing 的实现：累积容器即 *SummaryStats[N]。
type summarizeCollector[N constraints.Number] struct{}

// 编译期检查：summarizeCollector 实现 Collector。
var _ Collector[int, *SummaryStats[int], SummaryStats[int]] = summarizeCollector[int]{}

func (summarizeCollector[N]) Supplier() *SummaryStats[N] { return new(SummaryStats[N]) }
func (summarizeCollector[N]) Accumulator(s *SummaryStats[N], v N) {
	if s.Count == 0 {
		s.Min, s.Max = v, v
	} else {
		if v < s.Min {
			s.Min = v
		}
		if v > s.Max {
			s.Max = v
		}
	}
	s.Sum += v
	s.Count++
}

// Combiner 合并两片统计（空片让位）。
func (summarizeCollector[N]) Combiner() func(a, b *SummaryStats[N]) *SummaryStats[N] {
	return func(a, b *SummaryStats[N]) *SummaryStats[N] {
		if b.Count == 0 {
			return a
		}
		if a.Count == 0 {
			*a = *b
			return a
		}
		if b.Min < a.Min {
			a.Min = b.Min
		}
		if b.Max > a.Max {
			a.Max = b.Max
		}
		a.Sum += b.Sum
		a.Count += b.Count
		return a
	}
}

func (summarizeCollector[N]) Finisher(s *SummaryStats[N]) SummaryStats[N] { return *s }

// Summarizing 单遍数值统计收集器：一次遍历同时产出 count/sum/min/max
// （Avg 由 SummaryStats.Avg() 派生，免二次遍历）。
// 与 Summing/Averaging 同族；需要与其它收集器组合（如 GroupingByDownstream
// 分组统计）时用本形态，整流一行闭环用根包 stream.Summary。
func Summarizing[N constraints.Number]() Collector[N, *SummaryStats[N], SummaryStats[N]] {
	return summarizeCollector[N]{}
}
