// Package collector 提供流式汇聚的收集器族（Collector 接口及其预置实现）。
//
// 本包为 go-stream 的低耦合子包：仅依赖零依赖叶子包 constraints
// （共享数值约束）与标准库 strings。Collector 为接口，各收集器以
// 非导出具体类型实现：行为只能经接口方法调用，外部无法像旧 struct
// 函数字段那样改写其内部逻辑（防意外修改）。累积容器 A 均为指针
// 类型（如 *[]T、*map[K]V），使 Accumulator 的修改对 Finisher 可见。
package collector

import (
	"strings"

	"github.com/JayceChant/go-stream/constraints"
)

// Collector 是可组合的汇聚器接口。T 流元素类型；A 累积容器类型
// （指针，如 *map[K]V）；R 最终结果类型。方法均为每元素热路径或
// 每次 Collect 调用一次的低频路径，接口动态派发的开销由基准
// （BenchmarkCollect）回测把控。
type Collector[T, A, R any] interface {
	// Supplier 创建累积容器（每次 Collect / 并行每分片调用一次）。
	Supplier() A
	// Accumulator 把元素 v 累积进容器 a（每元素调用）。
	Accumulator(a A, v T)
	// Combiner 返回合并两累积容器的函数（并行分片合并，按分片序
	// 调用）；返回 nil 表示不支持并行合并，Collect 据此自动降级串行。
	Combiner() func(a, b A) A
	// Finisher 由累积容器产出最终结果（每次 Collect 调用一次）。
	Finisher(a A) R
}

// sliceCollector 是 ToSlice 的实现（无配置，空结构体）。
type sliceCollector[T any] struct{}

func (sliceCollector[T]) Supplier() *[]T { return &[]T{} }
func (sliceCollector[T]) Accumulator(a *[]T, v T) {
	*a = append(*a, v)
}

func (sliceCollector[T]) Combiner() func(a, b *[]T) *[]T {
	return func(a, b *[]T) *[]T {
		*a = append(*a, *b...)
		return a
	}
}
func (sliceCollector[T]) Finisher(a *[]T) []T { return *a }

// ToSlice 收集为切片。
func ToSlice[T any]() Collector[T, *[]T, []T] {
	return sliceCollector[T]{}
}

// setCollector 是 ToSet 的实现。
type setCollector[T comparable] struct{}

func (setCollector[T]) Supplier() *map[T]struct{} {
	m := make(map[T]struct{})
	return &m
}
func (setCollector[T]) Accumulator(a *map[T]struct{}, v T) { (*a)[v] = struct{}{} }
func (setCollector[T]) Combiner() func(a, b *map[T]struct{}) *map[T]struct{} {
	return func(a, b *map[T]struct{}) *map[T]struct{} {
		for k := range *b {
			(*a)[k] = struct{}{}
		}
		return a
	}
}
func (setCollector[T]) Finisher(a *map[T]struct{}) map[T]struct{} { return *a }

// ToSet 收集为集合（map[T]struct{}）。
func ToSet[T comparable]() Collector[T, *map[T]struct{}, map[T]struct{}] {
	return setCollector[T]{}
}

// mapCollector 是 ToMap/ToMapMerge 的实现：键冲突合并策略由
// merge 区分（ToMap 为 last-wins：直接返回新值）。
type mapCollector[K comparable, V any, T any] struct {
	keyF  func(T) K
	valF  func(T) V
	merge func(oldV, newV V) V
}

func (c mapCollector[K, V, T]) Supplier() *map[K]V {
	m := make(map[K]V)
	return &m
}

func (c mapCollector[K, V, T]) Accumulator(a *map[K]V, v T) {
	k, nv := c.keyF(v), c.valF(v)
	if ov, ok := (*a)[k]; ok {
		(*a)[k] = c.merge(ov, nv)
		return
	}
	(*a)[k] = nv
}

func (c mapCollector[K, V, T]) Combiner() func(a, b *map[K]V) *map[K]V {
	merge := c.merge
	return func(a, b *map[K]V) *map[K]V {
		for k, v := range *b {
			if ov, ok := (*a)[k]; ok {
				(*a)[k] = merge(ov, v)
			} else {
				(*a)[k] = v
			}
		}
		return a
	}
}
func (mapCollector[K, V, T]) Finisher(a *map[K]V) map[K]V { return *a }

// ToMap 收集为 map；键冲突 last-wins（对齐 Go map 赋值惯例）。
func ToMap[K comparable, V, T any](keyF func(T) K, valF func(T) V) Collector[T, *map[K]V, map[K]V] {
	return mapCollector[K, V, T]{
		keyF:  keyF,
		valF:  valF,
		merge: func(oldV, newV V) V { return newV },
	}
}

// ToMapMerge 与 ToMap 相同，但键冲突以 merge 合并（oldV 为已存在值，newV 为新值）。
func ToMapMerge[K comparable, V, T any](keyF func(T) K, valF func(T) V, merge func(oldV, newV V) V) Collector[T, *map[K]V, map[K]V] {
	return mapCollector[K, V, T]{keyF: keyF, valF: valF, merge: merge}
}

// groupCollector 是 GroupingBy 的实现。
type groupCollector[K comparable, V any, T any] struct {
	keyF func(T) K
	valF func(T) V
}

func (groupCollector[K, V, T]) Supplier() *map[K][]V {
	m := make(map[K][]V)
	return &m
}

func (c groupCollector[K, V, T]) Accumulator(a *map[K][]V, v T) {
	k := c.keyF(v)
	(*a)[k] = append((*a)[k], c.valF(v))
}

func (groupCollector[K, V, T]) Combiner() func(a, b *map[K][]V) *map[K][]V {
	return func(a, b *map[K][]V) *map[K][]V {
		for k, vs := range *b {
			(*a)[k] = append((*a)[k], vs...)
		}
		return a
	}
}
func (groupCollector[K, V, T]) Finisher(a *map[K][]V) map[K][]V { return *a }

// GroupingBy 按 keyF 分组为 map[K][]V，组内保持遇序。
func GroupingBy[K comparable, V, T any](keyF func(T) K, valF func(T) V) Collector[T, *map[K][]V, map[K][]V] {
	return groupCollector[K, V, T]{keyF: keyF, valF: valF}
}

// joinCollector 是 Joining 的实现；累积容器即 strings.Builder。
type joinCollector[T any] struct {
	strF func(T) string
	sep  string
}

func (joinCollector[T]) Supplier() *strings.Builder { return &strings.Builder{} }
func (c joinCollector[T]) Accumulator(b *strings.Builder, v T) {
	if b.Len() > 0 {
		b.WriteString(c.sep)
	}
	b.WriteString(c.strF(v))
}

func (c joinCollector[T]) Combiner() func(a, b *strings.Builder) *strings.Builder {
	return func(a, b *strings.Builder) *strings.Builder {
		if a.Len() > 0 {
			a.WriteString(c.sep)
		}
		a.WriteString(b.String())
		return a
	}
}
func (joinCollector[T]) Finisher(b *strings.Builder) string { return b.String() }

// Joining 把元素经 strF 映射后以 sep 拼接为字符串。
func Joining[T any](strF func(T) string, sep string) Collector[T, *strings.Builder, string] {
	return joinCollector[T]{strF: strF, sep: sep}
}

// countCollector 是 Counting 的实现；累积容器即 *int64。
type countCollector[T any] struct{}

func (countCollector[T]) Supplier() *int64          { return new(int64) }
func (countCollector[T]) Accumulator(n *int64, _ T) { *n++ }
func (countCollector[T]) Combiner() func(a, b *int64) *int64 {
	return func(a, b *int64) *int64 {
		*a += *b
		return a
	}
}
func (countCollector[T]) Finisher(n *int64) int64 { return *n }

// Counting 计数。
func Counting[T any]() Collector[T, *int64, int64] {
	return countCollector[T]{}
}

// reduceCollector 是 Reducing 的实现：identity 为初值，op 为折叠操作。
type reduceCollector[T any] struct {
	identity T
	op       func(T, T) T
}

func (c reduceCollector[T]) Supplier() *T {
	v := c.identity
	return &v
}
func (c reduceCollector[T]) Accumulator(a *T, v T) { *a = c.op(*a, v) }
func (c reduceCollector[T]) Combiner() func(a, b *T) *T {
	return func(a, b *T) *T {
		*a = c.op(*a, *b)
		return a
	}
}
func (reduceCollector[T]) Finisher(a *T) T { return *a }

// Reducing 以 identity 为初值折叠。
// 整流直接折叠用方法 s.Reduce（调用即求值）；本形态把折叠做成可传递的值，
// 用于收集器组合（如 Mapping(f, Reducing(...))、GroupingBy 分组后按组折叠）。
func Reducing[T any](identity T, op func(T, T) T) Collector[T, *T, T] {
	return reduceCollector[T]{identity: identity, op: op}
}

// mappingCollector 是 Mapping 的实现：转发 Supplier/Combiner/Finisher
// 至下游（Combiner 原样转发——含 nil，并行支持与下游一致），仅
// Accumulator 先经 f 变换再交下游累积。
type mappingCollector[T, U, A, R any] struct {
	f          func(T) U
	downstream Collector[U, A, R]
}

func (m mappingCollector[T, U, A, R]) Supplier() A          { return m.downstream.Supplier() }
func (m mappingCollector[T, U, A, R]) Accumulator(a A, v T) { m.downstream.Accumulator(a, m.f(v)) }
func (m mappingCollector[T, U, A, R]) Combiner() func(A, A) A {
	return m.downstream.Combiner()
}
func (m mappingCollector[T, U, A, R]) Finisher(a A) R { return m.downstream.Finisher(a) }

// Mapping 先经 f 变换元素，再交由 downstream 汇聚（收集器组合子）。
func Mapping[T, U, A, R any](f func(T) U, downstream Collector[U, A, R]) Collector[T, A, R] {
	return mappingCollector[T, U, A, R]{f: f, downstream: downstream}
}

// sumCollector 是 Summing 的实现；累积容器即 *N。
type sumCollector[N constraints.Number] struct{}

func (sumCollector[N]) Supplier() *N          { return new(N) }
func (sumCollector[N]) Accumulator(a *N, v N) { *a += v }
func (sumCollector[N]) Combiner() func(a, b *N) *N {
	return func(a, b *N) *N {
		*a += *b
		return a
	}
}
func (sumCollector[N]) Finisher(a *N) N { return *a }

// Summing 数值求和收集器（Number 约束的便捷形态）。
// 依赖共享的 constraints.Number 故能落入本子包；与根包 numeric.go
// 的 Sum/Avg 同属数值聚合族：整流直接求和用 stream.Sum（一行闭环，
// 免 import 本包与显式实例化）；需与其它收集器组合时用本形态
// （如 Mapping(f, Summing())、GroupingBy 分组后按组求和）。
func Summing[N constraints.Number]() Collector[N, *N, N] {
	return sumCollector[N]{}
}

// avgAcc 是 Averaging 的累积容器：和与计数同行累积（单遍完成均值）。
type avgAcc[N constraints.Number] struct {
	sum N
	n   int64
}

// avgCollector 是 Averaging 的实现。
type avgCollector[N constraints.Number] struct{}

func (avgCollector[N]) Supplier() *avgAcc[N] { return new(avgAcc[N]) }
func (avgCollector[N]) Accumulator(a *avgAcc[N], v N) {
	a.sum += v
	a.n++
}

func (avgCollector[N]) Combiner() func(a, b *avgAcc[N]) *avgAcc[N] {
	return func(a, b *avgAcc[N]) *avgAcc[N] {
		a.sum += b.sum
		a.n += b.n
		return a
	}
}

func (avgCollector[N]) Finisher(a *avgAcc[N]) N {
	if a.n == 0 {
		return 0
	}
	return a.sum / N(a.n)
}

// Averaging 数值平均收集器（Number 约束）。
// 与 Summing 同构：整流直接求平均用 stream.Avg（一行闭环、免跨包）；
// 需与其它收集器组合时用本形态（如 Mapping(f, Averaging())、
// GroupingBy 分组后按组平均）。
// 空流返回 0（与 stream.Avg 一致）；整型按整除语义截断。
func Averaging[N constraints.Number]() Collector[N, *avgAcc[N], N] {
	return avgCollector[N]{}
}
