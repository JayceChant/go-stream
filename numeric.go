package stream

import "cmp"

// numeric.go：需要元素类型约束的包级便捷操作。
//
// Go 1.27 限制：方法不能约束接收者已有的类型参数（spec「Go 1.27 泛型方法的关键
// 约束」第 4 条），需要约束 T 本身的 API 只能以包级函数提供；并存≠重复，
// 各函数与最近替代形态的取舍见各自 godoc。

// Sum 数值求和。
// 与 s.Collect(collectors.Summing[N]()) 等价：保留根包一行闭环，
// 高频终端操作免跨包 import collector 与显式实例化。
func Sum[N Number](s *Stream[N]) N {
	var acc N
	s.pipeline.evaluate(sinkFunc[N](func(v N) bool { acc += v; return true }))
	return acc
}

// Avg 数值平均（空流返回 0）。
// 单遍求值：和与计数同行累积。收集器形态见 collector.Averaging
// （需与其它收集器组合时用）。
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
// 与 s.AnyMatch(func(v T) bool { return v == target }) 等价：免写样板闭包，
// 且 nil 流安全返回 false；any 约束的方法体内无法使用 ==，comparable 只能落在包级。
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

// Sorted 依自然序（cmp.Ordered）排序（不稳定，委托方法 Sorted）。
// 免写比较器形态：方法版须手写 cmp.Compare[T]，方法无法对 T 追加
// cmp.Ordered 约束，故落在包级。需要稳定排序时用 s.StableSorted(cmp.Compare[T])。
func Sorted[T cmp.Ordered](s *Stream[T]) *Stream[T] {
	if s == nil {
		return nil
	}
	return s.Sorted(cmp.Compare[T])
}

// Min 依自然序取最小（空流返回零值与 false）。
// 免写比较器形态：与 s.Min(cmp.Compare[T]) 等价，方法无法约束 T 故落在包级。
func Min[T cmp.Ordered](s *Stream[T]) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	return s.Min(cmp.Compare[T])
}

// Max 依自然序取最大（空流返回零值与 false）。
// 免写比较器形态：与 s.Max(cmp.Compare[T]) 等价，方法无法约束 T 故落在包级。
func Max[T cmp.Ordered](s *Stream[T]) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	return s.Max(cmp.Compare[T])
}
