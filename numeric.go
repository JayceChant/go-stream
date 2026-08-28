package stream

import "cmp"

// numeric.go：需要元素类型约束的包级便捷操作。
//
// Go 1.27 限制：方法无法对接收者的 T 追加 cmp.Ordered/Number 约束，
// 故数值聚合与"免写比较器"形态均以包级函数提供。

// Sum 数值求和。
func Sum[N Number](s *Stream[N]) N {
	var acc N
	s.pipeline.evaluate(sinkFunc[N](func(v N) bool { acc += v; return true }))
	return acc
}

// Avg 数值平均（空流返回 0）。
func Avg[N Number](s *Stream[N]) N {
	var acc N
	var n int64
	s.pipeline.evaluate(sinkFunc[N](func(v N) bool { acc += v; n++; return true }))
	if n == 0 {
		return 0
	}
	return acc / N(n)
}

// Contains 判断流中是否含有目标元素（短路）。
func Contains[T comparable](s *Stream[T], target T) bool {
	if s == nil {
		return false
	}
	found := false
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		if v == target {
			found = true
			return false
		}
		return true
	}))
	return found
}

// Sorted 依自然序（cmp.Ordered）稳定排序。
// 包级函数形态：方法无法对 T 追加 cmp.Ordered 约束。
func Sorted[T cmp.Ordered](s *Stream[T]) *Stream[T] {
	if s == nil {
		return nil
	}
	return s.Sorted(cmp.Compare[T])
}

// Min 依自然序取最小（空流返回零值与 false）。
func Min[T cmp.Ordered](s *Stream[T]) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	return s.Min(cmp.Compare[T])
}

// Max 依自然序取最大（空流返回零值与 false）。
func Max[T cmp.Ordered](s *Stream[T]) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	return s.Max(cmp.Compare[T])
}
