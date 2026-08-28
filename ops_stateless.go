package stream

import "iter"

// ops_stateless.go：无状态中间操作（求值时融合为单遍，不物化）。
//
// 特征位传播（对齐 Java StreamOpFlag 语义，Task 6 修订）：
//   - Filter：保留上游全部特征位（过滤不改变结构性质）
//   - Map/MapErr（1:1 变换）：保留 SpSized（下游可按 size 预分配），
//     清除 SpDistinct/SpSorted（元素集已改变，去重与有序不再成立）
//   - FlatMap 族/Scan/Enumerate 等元素变换：清除 SpSized/SpDistinct/SpSorted
//     （1:N 变换，数量不再精确）
//   - TakeWhile/DropWhile：清除 SpSized（截断后数量未知），其余保留

// Filter 保留满足谓词 p 的元素。
func (s *Stream[T]) Filter(p func(T) bool) *Stream[T] {
	if p == nil {
		panic("stream: Filter 谓词为 nil")
	}
	return newStateless(s, func(down Sink[T], _ *evalCtx) Sink[T] {
		return &filterSink[T]{down: down, p: p}
	}, s.chars)
}

type filterSink[T any] struct {
	down Sink[T]
	p    func(T) bool
}

func (w *filterSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *filterSink[T]) Accept(v T) bool {
	if w.p(v) {
		return w.down.Accept(v)
	}
	return true // 未通过过滤：继续拉取，不取消
}
func (w *filterSink[T]) End() { w.down.End() }

// Map 将每个元素经 f 变换为新类型 U（泛型方法，元素类型迁移）。
// 1:1 变换：保留 SpSized（下游可按 size 预分配），仅清 SpSorted/SpDistinct。
func (s *Stream[T]) Map[U any](f func(T) U) *Stream[U] {
	if f == nil {
		panic("stream: Map 函数为 nil")
	}
	return newStateless(s, func(down Sink[U], _ *evalCtx) Sink[T] {
		return &mapSink[T, U]{down: down, f: f}
	}, s.chars&^(SpDistinct|SpSorted))
}

type mapSink[T, U any] struct {
	down Sink[U]
	f    func(T) U
}

func (w *mapSink[T, U]) Begin(size int64) { w.down.Begin(size) }
func (w *mapSink[T, U]) Accept(v T) bool  { return w.down.Accept(w.f(v)) }
func (w *mapSink[T, U]) End()             { w.down.End() }

// FlatMap 将每个元素经 f 展开为子序列并依次输出。
func (s *Stream[T]) FlatMap[U any](f func(T) []U) *Stream[U] {
	if f == nil {
		panic("stream: FlatMap 函数为 nil")
	}
	return newStateless(s, func(down Sink[U], _ *evalCtx) Sink[T] {
		return &flatMapSink[T, U]{down: down, f: f}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
}

type flatMapSink[T, U any] struct {
	down Sink[U]
	f    func(T) []U
}

func (w *flatMapSink[T, U]) Begin(int64) { w.down.Begin(-1) }
func (w *flatMapSink[T, U]) Accept(v T) bool {
	for _, u := range w.f(v) {
		if !w.down.Accept(u) {
			return false
		}
	}
	return true
}
func (w *flatMapSink[T, U]) End() { w.down.End() }

// FlatMapSeq 与 FlatMap 相同，但展开函数返回 iter.Seq（支持惰性子序列）。
func (s *Stream[T]) FlatMapSeq[U any](f func(T) iter.Seq[U]) *Stream[U] {
	if f == nil {
		panic("stream: FlatMapSeq 函数为 nil")
	}
	return newStateless(s, func(down Sink[U], _ *evalCtx) Sink[T] {
		return &flatMapSeqSink[T, U]{down: down, f: f}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
}

type flatMapSeqSink[T, U any] struct {
	down Sink[U]
	f    func(T) iter.Seq[U]
}

func (w *flatMapSeqSink[T, U]) Begin(int64) { w.down.Begin(-1) }
func (w *flatMapSeqSink[T, U]) Accept(v T) bool {
	cont := true
	w.f(v)(func(u U) bool {
		cont = w.down.Accept(u)
		return cont
	})
	return cont
}
func (w *flatMapSeqSink[T, U]) End() { w.down.End() }

// Peek 对每个元素施加副作用 f（不改变元素，常用于调试观察）。
// 并行流下 f 在分片 goroutine 内执行，观察顺序不保证（需保序请用 ForEach）。
func (s *Stream[T]) Peek(f func(T)) *Stream[T] {
	if f == nil {
		panic("stream: Peek 函数为 nil")
	}
	return newStateless(s, func(down Sink[T], _ *evalCtx) Sink[T] {
		return &peekSink[T]{down: down, f: f}
	}, s.chars)
}

type peekSink[T any] struct {
	down Sink[T]
	f    func(T)
}

func (w *peekSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *peekSink[T]) Accept(v T) bool  { w.f(v); return w.down.Accept(v) }
func (w *peekSink[T]) End()             { w.down.End() }

// TakeWhile 保留首批满足 p 的元素，遇到首个不满足即终止（短路）。
func (s *Stream[T]) TakeWhile(p func(T) bool) *Stream[T] {
	if p == nil {
		panic("stream: TakeWhile 谓词为 nil")
	}
	return newStateless(s, func(down Sink[T], _ *evalCtx) Sink[T] {
		return &takeWhileSink[T]{down: down, p: p}
	}, s.chars & ^SpSized)
}

type takeWhileSink[T any] struct {
	down Sink[T]
	p    func(T) bool
}

func (w *takeWhileSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *takeWhileSink[T]) Accept(v T) bool {
	if w.p(v) {
		return w.down.Accept(v)
	}
	return false // 首个不满足：取消整条链
}
func (w *takeWhileSink[T]) End() { w.down.End() }

// DropWhile 丢弃首批满足 p 的元素，之后全部放行。
// 有状态单遍（done 门闸）→ 并行降级（splitN=nil）。
func (s *Stream[T]) DropWhile(p func(T) bool) *Stream[T] {
	if p == nil {
		panic("stream: DropWhile 谓词为 nil")
	}
	ns := newStateless(s, func(down Sink[T], _ *evalCtx) Sink[T] {
		return &dropWhileSink[T]{down: down, p: p}
	}, s.chars & ^SpSized)
	ns.splitN = nil
	return ns
}

type dropWhileSink[T any] struct {
	down Sink[T]
	p    func(T) bool
	done bool
}

func (w *dropWhileSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *dropWhileSink[T]) Accept(v T) bool {
	if !w.done {
		if w.p(v) {
			return true // 仍在丢弃阶段：继续拉取
		}
		w.done = true
	}
	return w.down.Accept(v)
}
func (w *dropWhileSink[T]) End() { w.down.End() }

// ---- Err 变体（错误即值：首错短路 + 部分结果保留，详见 spec 错误模型）----

// MapErr 带错误返回的 Map：f 出错时记录首错并终止求值。
// 1:1 变换：保留 SpSized，仅清 SpSorted/SpDistinct。
func (s *Stream[T]) MapErr[U any](f func(T) (U, error)) *Stream[U] {
	if f == nil {
		panic("stream: MapErr 函数为 nil")
	}
	return newStateless(s, func(down Sink[U], ec *evalCtx) Sink[T] {
		return &mapErrSink[T, U]{down: down, ec: ec, f: f}
	}, s.chars&^(SpDistinct|SpSorted))
}

type mapErrSink[T, U any] struct {
	down Sink[U]
	ec   *evalCtx
	f    func(T) (U, error)
}

func (w *mapErrSink[T, U]) Begin(size int64) { w.down.Begin(size) }
func (w *mapErrSink[T, U]) Accept(v T) bool {
	u, err := w.f(v)
	if err != nil {
		return w.ec.fail(err) // 记录首错 + 短路
	}
	return w.down.Accept(u)
}
func (w *mapErrSink[T, U]) End() { w.down.End() }

// FilterErr 带错误返回的 Filter。
func (s *Stream[T]) FilterErr(p func(T) (bool, error)) *Stream[T] {
	if p == nil {
		panic("stream: FilterErr 谓词为 nil")
	}
	return newStateless(s, func(down Sink[T], ec *evalCtx) Sink[T] {
		return &filterErrSink[T]{down: down, ec: ec, p: p}
	}, s.chars)
}

type filterErrSink[T any] struct {
	down Sink[T]
	ec   *evalCtx
	p    func(T) (bool, error)
}

func (w *filterErrSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *filterErrSink[T]) Accept(v T) bool {
	ok, err := w.p(v)
	if err != nil {
		return w.ec.fail(err)
	}
	if ok {
		return w.down.Accept(v)
	}
	return true
}
func (w *filterErrSink[T]) End() { w.down.End() }

// FlatMapErr 带错误返回的 FlatMap。
func (s *Stream[T]) FlatMapErr[U any](f func(T) ([]U, error)) *Stream[U] {
	if f == nil {
		panic("stream: FlatMapErr 函数为 nil")
	}
	return newStateless(s, func(down Sink[U], ec *evalCtx) Sink[T] {
		return &flatMapErrSink[T, U]{down: down, ec: ec, f: f}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
}

type flatMapErrSink[T, U any] struct {
	down Sink[U]
	ec   *evalCtx
	f    func(T) ([]U, error)
}

func (w *flatMapErrSink[T, U]) Begin(int64) { w.down.Begin(-1) }
func (w *flatMapErrSink[T, U]) Accept(v T) bool {
	us, err := w.f(v)
	if err != nil {
		return w.ec.fail(err)
	}
	for _, u := range us {
		if !w.down.Accept(u) {
			return false
		}
	}
	return true
}
func (w *flatMapErrSink[T, U]) End() { w.down.End() }

// PeekErr 带错误返回的 Peek。
func (s *Stream[T]) PeekErr(f func(T) error) *Stream[T] {
	if f == nil {
		panic("stream: PeekErr 函数为 nil")
	}
	return newStateless(s, func(down Sink[T], ec *evalCtx) Sink[T] {
		return &peekErrSink[T]{down: down, ec: ec, f: f}
	}, s.chars)
}

type peekErrSink[T any] struct {
	down Sink[T]
	ec   *evalCtx
	f    func(T) error
}

func (w *peekErrSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *peekErrSink[T]) Accept(v T) bool {
	if err := w.f(v); err != nil {
		return w.ec.fail(err)
	}
	return w.down.Accept(v)
}
func (w *peekErrSink[T]) End() { w.down.End() }
