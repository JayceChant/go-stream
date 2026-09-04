// Package collector 提供流式汇聚的收集器族（Collector 及其预置实现）。
//
// 本包为 go-stream 的低耦合子包：仅依赖零依赖叶子包 constraints
// （共享数值约束）与标准库 strings（Collector 以 struct + 函数字段
// 组合装配，供根包 Stream.Collect 消费）。累积容器 A 均为指针类型
// （如 *[]T、*map[K]V），使 Accumulator 的修改对 Finisher 可见；
// Combiner 供并行求值按分片序合并。
package collector

import (
	"strings"

	"github.com/JayceChant/go-stream/constraints"
)

// Collector 是可组合的汇聚器。T 流元素类型；A 累积容器类型（建议指针）；
// R 最终结果类型。
type Collector[T, A, R any] struct {
	Supplier    func() A
	Accumulator func(A, T)
	Combiner    func(A, A) A
	Finisher    func(A) R
}

// ToSlice 收集为切片。
func ToSlice[T any]() Collector[T, *[]T, []T] {
	return Collector[T, *[]T, []T]{
		Supplier: func() *[]T { return &[]T{} },
		Accumulator: func(a *[]T, v T) {
			*a = append(*a, v)
		},
		Combiner: func(a, b *[]T) *[]T {
			*a = append(*a, *b...)
			return a
		},
		Finisher: func(a *[]T) []T { return *a },
	}
}

// ToSet 收集为集合（map[T]struct{}）。
func ToSet[T comparable]() Collector[T, *map[T]struct{}, map[T]struct{}] {
	return Collector[T, *map[T]struct{}, map[T]struct{}]{
		Supplier: func() *map[T]struct{} {
			m := make(map[T]struct{})
			return &m
		},
		Accumulator: func(a *map[T]struct{}, v T) { (*a)[v] = struct{}{} },
		Combiner: func(a, b *map[T]struct{}) *map[T]struct{} {
			for k := range *b {
				(*a)[k] = struct{}{}
			}
			return a
		},
		Finisher: func(a *map[T]struct{}) map[T]struct{} { return *a },
	}
}

// ToMap 收集为 map；键冲突 last-wins（对齐 Go map 赋值惯例）。
func ToMap[K comparable, V, T any](keyF func(T) K, valF func(T) V) Collector[T, *map[K]V, map[K]V] {
	return ToMapMerge(keyF, valF, func(oldV, newV V) V { return newV })
}

// ToMapMerge 与 ToMap 相同，但键冲突以 merge 合并（oldV 为已存在值，newV 为新值）。
func ToMapMerge[K comparable, V, T any](keyF func(T) K, valF func(T) V, merge func(oldV, newV V) V) Collector[T, *map[K]V, map[K]V] {
	return Collector[T, *map[K]V, map[K]V]{
		Supplier: func() *map[K]V {
			m := make(map[K]V)
			return &m
		},
		Accumulator: func(a *map[K]V, v T) {
			k, nv := keyF(v), valF(v)
			if ov, ok := (*a)[k]; ok {
				(*a)[k] = merge(ov, nv)
				return
			}
			(*a)[k] = nv
		},
		Combiner: func(a, b *map[K]V) *map[K]V {
			for k, v := range *b {
				if ov, ok := (*a)[k]; ok {
					(*a)[k] = merge(ov, v)
				} else {
					(*a)[k] = v
				}
			}
			return a
		},
		Finisher: func(a *map[K]V) map[K]V { return *a },
	}
}

// GroupingBy 按 keyF 分组为 map[K][]V，组内保持遇序。
func GroupingBy[K comparable, V, T any](keyF func(T) K, valF func(T) V) Collector[T, *map[K][]V, map[K][]V] {
	return Collector[T, *map[K][]V, map[K][]V]{
		Supplier: func() *map[K][]V {
			m := make(map[K][]V)
			return &m
		},
		Accumulator: func(a *map[K][]V, v T) {
			k := keyF(v)
			(*a)[k] = append((*a)[k], valF(v))
		},
		Combiner: func(a, b *map[K][]V) *map[K][]V {
			for k, vs := range *b {
				(*a)[k] = append((*a)[k], vs...)
			}
			return a
		},
		Finisher: func(a *map[K][]V) map[K][]V { return *a },
	}
}

// Joining 把元素经 strF 映射后以 sep 拼接为字符串。
func Joining[T any](strF func(T) string, sep string) Collector[T, *strings.Builder, string] {
	return Collector[T, *strings.Builder, string]{
		Supplier: func() *strings.Builder { return &strings.Builder{} },
		Accumulator: func(b *strings.Builder, v T) {
			if b.Len() > 0 {
				b.WriteString(sep)
			}
			b.WriteString(strF(v))
		},
		Combiner: func(a, b *strings.Builder) *strings.Builder {
			if a.Len() > 0 {
				a.WriteString(sep)
			}
			a.WriteString(b.String())
			return a
		},
		Finisher: func(b *strings.Builder) string { return b.String() },
	}
}

// Counting 计数。
func Counting[T any]() Collector[T, *int64, int64] {
	return Collector[T, *int64, int64]{
		Supplier:    func() *int64 { return new(int64) },
		Accumulator: func(n *int64, _ T) { *n++ },
		Combiner: func(a, b *int64) *int64 {
			*a += *b
			return a
		},
		Finisher: func(n *int64) int64 { return *n },
	}
}

// Reducing 以 identity 为初值折叠。
// 整流直接折叠用方法 s.Reduce（调用即求值）；本形态把折叠做成可传递的值，
// 用于收集器组合（如 Mapping(f, Reducing(...))、GroupingBy 分组后按组折叠）。
func Reducing[T any](identity T, op func(T, T) T) Collector[T, *T, T] {
	return Collector[T, *T, T]{
		Supplier: func() *T {
			v := identity
			return &v
		},
		Accumulator: func(a *T, v T) { *a = op(*a, v) },
		Combiner: func(a, b *T) *T {
			*a = op(*a, *b)
			return a
		},
		Finisher: func(a *T) T { return *a },
	}
}

// Mapping 先经 f 变换元素，再交由 downstream 汇聚（收集器组合子）。
func Mapping[T, U, A, R any](f func(T) U, downstream Collector[U, A, R]) Collector[T, A, R] {
	return Collector[T, A, R]{
		Supplier:    downstream.Supplier,
		Accumulator: func(a A, v T) { downstream.Accumulator(a, f(v)) },
		Combiner:    downstream.Combiner,
		Finisher:    downstream.Finisher,
	}
}

// Summing 数值求和收集器（Number 约束的便捷形态）。
// 依赖共享的 constraints.Number 故能落入本子包；与根包 numeric.go
// 的 Sum/Avg 同属数值聚合族：整流直接求和用 stream.Sum（一行闭环，
// 免 import 本包与显式实例化）；需与其它收集器组合时用本形态
// （如 Mapping(f, Summing())、GroupingBy 分组后按组求和）。
func Summing[N constraints.Number]() Collector[N, *N, N] {
	return Collector[N, *N, N]{
		Supplier:    func() *N { return new(N) },
		Accumulator: func(a *N, v N) { *a += v },
		Combiner: func(a, b *N) *N {
			*a += *b
			return a
		},
		Finisher: func(a *N) N { return *a },
	}
}

// avgAcc 是 Averaging 的累积容器：和与计数同行累积（单遍完成均值）。
type avgAcc[N constraints.Number] struct {
	sum N
	n   int64
}

// Averaging 数值平均收集器（Number 约束）。
// 与 Summing 同构：整流直接求平均用 stream.Avg（一行闭环、免跨包）；
// 需与其它收集器组合时用本形态（如 Mapping(f, Averaging())、
// GroupingBy 分组后按组平均）。
// 空流返回 0（与 stream.Avg 一致）；整型按整除语义截断。
func Averaging[N constraints.Number]() Collector[N, *avgAcc[N], N] {
	return Collector[N, *avgAcc[N], N]{
		Supplier:    func() *avgAcc[N] { return new(avgAcc[N]) },
		Accumulator: func(a *avgAcc[N], v N) { a.sum += v; a.n++ },
		Combiner: func(a, b *avgAcc[N]) *avgAcc[N] {
			a.sum += b.sum
			a.n += b.n
			return a
		},
		Finisher: func(a *avgAcc[N]) N {
			if a.n == 0 {
				return 0
			}
			return a.sum / N(a.n)
		},
	}
}
