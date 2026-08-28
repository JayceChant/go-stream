package stream

import (
	"strings"
)

// collector.go：Collector 汇聚抽象与预置收集器族。
//
// Collector 以 struct + 函数字段组合装配（非接口多态）。累积容器 A 均为
// 指针类型（如 *[]T、*map[K]V），使 Accumulator 的修改对 Finisher 可见；
// Combiner 供后续并行求值合并分片结果（串行下可为 nil）。

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
	return ToMapMerge(keyF, valF, func(old, new V) V { return new })
}

// ToMapMerge 与 ToMap 相同，但键冲突以 merge 合并（old 为已存在值，new 为新值）。
func ToMapMerge[K comparable, V, T any](keyF func(T) K, valF func(T) V, merge func(old, new V) V) Collector[T, *map[K]V, map[K]V] {
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

// Summing 数值求和（Number 约束的便捷收集器）。
func Summing[N Number]() Collector[N, *N, N] {
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
