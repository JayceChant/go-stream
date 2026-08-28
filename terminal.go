package stream

// terminal.go：终止操作（触发一次求值，返回新容器或聚合值）。
//
// 短路终止（First/AnyMatch/AllMatch/NoneMatch）在条件满足时立即停止源遍历；
// 出错时返回已累积的部分结果（错误即值模型），Err() 查询首错。

// ForEach 对每个元素执行 f。
func (s *Stream[T]) ForEach(f func(T)) {
	if f == nil {
		panic("stream: ForEach 函数为 nil")
	}
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool { f(v); return true }))
}

// ForEachUntil 对每个元素执行 f；f 返回 false 时提前终止。
func (s *Stream[T]) ForEachUntil(f func(T) bool) {
	if f == nil {
		panic("stream: ForEachUntil 函数为 nil")
	}
	s.pipeline.evaluate(sinkFunc[T](f))
}

// ToSlice 收集全部元素为新切片。
func (s *Stream[T]) ToSlice() []T {
	cs := &collectingSink[T]{limit: -1}
	s.pipeline.evaluate(cs)
	return cs.buf
}

// Count 返回元素总数。
func (s *Stream[T]) Count() int64 {
	var n int64
	s.pipeline.evaluate(sinkFunc[T](func(T) bool { n++; return true }))
	return n
}

// Reduce 以 identity 为初值折叠全部元素。
func (s *Stream[T]) Reduce(identity T, op func(T, T) T) T {
	if op == nil {
		panic("stream: Reduce 操作为 nil")
	}
	acc := identity
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool { acc = op(acc, v); return true }))
	return acc
}

// ReduceOpt 无初值折叠：空流返回 (零值, false)。
func (s *Stream[T]) ReduceOpt(op func(T, T) T) (T, bool) {
	if op == nil {
		panic("stream: ReduceOpt 操作为 nil")
	}
	var acc T
	found := false
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		if !found {
			acc, found = v, true
			return true
		}
		acc = op(acc, v)
		return true
	}))
	return acc, found
}

// First 返回首个元素（短路：取到即停）；空流返回 (零值, false)。
func (s *Stream[T]) First() (T, bool) {
	var first T
	found := false
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		first, found = v, true
		return false // 短路
	}))
	return first, found
}

// FindAny 寻找任一满足 p 的元素（短路）。顺序流下等价于 First + Filter。
func (s *Stream[T]) FindAny(p func(T) bool) (T, bool) {
	if p == nil {
		panic("stream: FindAny 谓词为 nil")
	}
	var hit T
	found := false
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		if p(v) {
			hit, found = v, true
			return false
		}
		return true
	}))
	return hit, found
}

// AnyMatch 是否存在满足 p 的元素（短路：命中即返回 true）。
func (s *Stream[T]) AnyMatch(p func(T) bool) bool {
	if p == nil {
		panic("stream: AnyMatch 谓词为 nil")
	}
	return s.match(p, true, false)
}

// AllMatch 是否全部元素满足 p（短路：遇首个不满足返回 false；空流 true）。
func (s *Stream[T]) AllMatch(p func(T) bool) bool {
	if p == nil {
		panic("stream: AllMatch 谓词为 nil")
	}
	return s.match(p, false, true)
}

// NoneMatch 是否无元素满足 p（空流 true）。
func (s *Stream[T]) NoneMatch(p func(T) bool) bool {
	if p == nil {
		panic("stream: NoneMatch 谓词为 nil")
	}
	return !s.AnyMatch(p)
}

// match 是 match 族终止操作的公共实现。
// stopOn 为 true：谓词结果等于 expect 时短路返回；否则谓词结果不等于 expect 时短路。
func (s *Stream[T]) match(p func(T) bool, stopOn bool, expect bool) bool {
	result := expect // 空流返回值
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		if p(v) == stopOn {
			result = !expect
			return false
		}
		return true
	}))
	return result
}

// Min 返回最小元素（依 cmp）；空流返回 (零值, false)。
func (s *Stream[T]) Min(cmp func(a, b T) int) (T, bool) {
	return s.minmax(cmp, -1)
}

// Max 返回最大元素（依 cmp）；空流返回 (零值, false)。
func (s *Stream[T]) Max(cmp func(a, b T) int) (T, bool) {
	return s.minmax(cmp, 1)
}

func (s *Stream[T]) minmax(cmp func(a, b T) int, sign int) (T, bool) {
	if cmp == nil {
		panic("stream: Min/Max 比较器为 nil")
	}
	var best T
	found := false
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		if !found {
			best, found = v, true
			return true
		}
		if cmp(v, best)*sign > 0 {
			best = v
		}
		return true
	}))
	return best, found
}

// Err 返回最近一次由本流发起的终止求值的首错（错误即值模型）。
// 无错误返回 nil；未求值前调用亦返回 nil。
func (s *Stream[T]) Err() error {
	return s.pipeline.err
}

// Collect 以自定义收集器汇聚元素（泛型方法，支持 A→R 类型迁移）。
func (s *Stream[T]) Collect[A, R any](c Collector[T, A, R]) R {
	a := c.Supplier()
	s.pipeline.evaluate(sinkFunc[T](func(v T) bool {
		c.Accumulator(a, v)
		return true
	}))
	return c.Finisher(a)
}
