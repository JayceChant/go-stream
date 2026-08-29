package stream

import "sync"

// op_ext.go：双流算子（Zip）与需要元素类型约束的包级便捷算子。
//
// Go 1.27 限制：方法无法对接收者的 T 追加 comparable 约束，因此
// "天然去重"只能以包级函数提供（调用方写 stream.Distinct(s)）。

// Zip 把两条流按位置配对：以 f 合并本流元素与 other 对应元素，
// 任一流耗尽即终止（取短）。两条流均被标记消费。
func (s *Stream[T]) Zip[U, R any](other *Stream[U], f func(T, U) R) *Stream[R] {
	if other == nil {
		panic("stream: Zip 另一流为 nil")
	}
	if f == nil {
		panic("stream: Zip 合并函数为 nil")
	}
	s.checkLinked()
	other.checkLinked()
	sd := s.drive
	chars := s.chars & other.chars &^ (SpSized | SpSorted | SpDistinct)
	return &Stream[R]{pipeline[R]{
		drive: func(down Sink[R], ec *evalCtx) {
			// other 转为拉取式（后台 goroutine + 单缓冲通道），
			// 本流在当前 goroutine 驱动，逐元素配对（取短）。
			next, stop := pullFromDrive(other.drive, ec)
			defer stop() // 本流先耗尽/短路时停止后台，防 goroutine 泄漏
			sd(sinkFunc[T](func(t T) bool {
				u, ok := next()
				if !ok {
					return false // other 耗尽：本流随之终止（取短）
				}
				return down.Accept(f(t, u))
			}), ec)
			if p := ec.takePanic(); p != nil {
				panic(p) // 后台 goroutine 的用户回调 panic 原样传播
			}
		},
		chars: chars,
		// Zip 后台 goroutine 拉取式双流，不参与并行分片（splitN 降级为 nil）
		closers: mergeClosers(s.closers, other.closers), // 双方回调链按本流先 other 后继承
	}}
}

// pullFromDrive 把 push 式 drive 闭包转为拉取函数 next（每调用取一个元素）。
// next 在耗尽后恒返回 false；stop 释放后台 goroutine（重复调用安全）。
//
// 后台 goroutine 中的用户回调 panic 被 recover 后暂存 ec.panicVal，
// 由发起侧在求值收尾处重新 panic（原样传播语义）。
func pullFromDrive[T any](drive func(Sink[T], *evalCtx), ec *evalCtx) (next func() (T, bool), stop func()) {
	ch := make(chan T)
	done := make(chan struct{})
	var once sync.Once
	stop = func() { once.Do(func() { close(done) }) }

	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				ec.mu.Lock()
				if ec.panicVal == nil {
					ec.panicVal = r
				}
				ec.mu.Unlock()
			}
		}()
		drive(sinkFunc[T](func(v T) bool {
			select {
			case ch <- v:
				return true
			case <-done: // 下游已终止：停止推送
				return false
			}
		}), ec)
	}()

	next = func() (T, bool) {
		// 只从 ch 接收：后台 drain 结束/被 stop 后关闭 ch 是唯一终止信号，
		// 不监听 done（否则与后台的 ch<-v 并发，产生竞态并泄漏 goroutine）。
		v, ok := <-ch
		return v, ok
	}
	return next, stop
}

// Distinct 依据元素自身可比较性去重（保留首见，保持遇序）。
// 包级函数形态：Go 方法无法对 T 追加 comparable 约束。
func Distinct[T comparable](s *Stream[T]) *Stream[T] {
	if s == nil {
		return nil
	}
	return s.DistinctBy(func(t T) any { return t })
}

// Chunk 把连续元素切分为定长分组（尾组可能不足 n）。n <= 0 panic。
//
// 包级函数形态：Go 1.27 泛型方法返回 Stream[[]T]（T 的派生类型）会触发
// 实例化循环（T → []T → [][]T → ...），只能以包级函数提供。
// 有状态单遍（跨元素缓冲）→ 并行降级（splitN=nil）。
func Chunk[T any](s *Stream[T], n int) *Stream[[]T] {
	if n <= 0 {
		panic("stream: Chunk 分组大小必须为正")
	}
	ns := newStateless(s, func(down Sink[[]T], _ *evalCtx) Sink[T] {
		return &chunkSink[T]{down: down, n: n}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
	ns.splitN = nil
	return ns
}

// Enumerate 为元素附加从 0 开始的索引，产出 KV[int, T]
// （对应 Go for i, v := range 习惯）。
//
// 包级函数形态：同 Chunk，泛型方法返回 Stream[KV[int, T]] 触发实例化循环。
// 有状态单遍（递增索引）→ 并行降级（splitN=nil）。
func Enumerate[T any](s *Stream[T]) *Stream[KV[int, T]] {
	ns := newStateless(s, func(down Sink[KV[int, T]], _ *evalCtx) Sink[T] {
		return &enumerateSink[T]{down: down}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
	ns.splitN = nil
	return ns
}
