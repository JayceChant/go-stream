package stream

import "sync"

// lifecycle.go：生命周期与可重放（Task 10）。
//
// 两项原路线图遗留在此实现（第三项 Unordered 流式合并见 parallel.go）：
// OnClose/Close 资源管理与 Cache 可重放工厂。
// 语义详见 spec「生命周期与可重放（Task 10）」Requirement。

// OnClose 注册资源清理回调 f：返回携带回调链的新流（中间操作语义：
// 消费本流，已注册的回调链一并继承）。f 为 nil 时 panic（编程错误）。
//
// 触发时机：新流（或其任一下游）的终止求值结束时自动触发一次——
// 正常耗尽、短路与错误值路径均触发，用户回调 panic 的展开路径亦触发；
// 求值前显式调用过 Close 则以显式关闭为准。多个回调按注册顺序执行；
// 任一出错记录首错（不 panic，可经 Err() 查询）。
//
// 幂等保证：每个物理回调以 sync.Once 包装——无论经由求值自动触发、
// 任一 stage 实例的显式 Close、还是组合流（Concat/Zip 继承合并后的
// 回调链）触发，均恰好执行一次。
func (s *Stream[T]) OnClose(f func() error) *Stream[T] {
	if f == nil {
		panic("stream: OnClose 回调为 nil")
	}
	var once sync.Once
	var ferr error
	g := func() error {
		once.Do(func() { ferr = f() })
		return ferr
	}
	ns := s.newFlagStage(s.parN)
	ns.closers = append(append([]func() error{}, s.closers...), g)
	return ns
}

// Close 显式关闭本流：立即触发回调链（幂等——重复调用、求值后再 Close
// 均不重复执行；未求值的流也可关闭）。返回回调链首错，并记入错误槽
// （Err() 可查询）。
func (s *Stream[T]) Close() error {
	p := &s.pipeline
	p.runClosers()
	if p.closeErr != nil && p.err == nil {
		p.err = p.closeErr
	}
	return p.closeErr
}

// mergeClosers 合并两条流的回调链（按 a 先 b 后的求值序），
// 供组合流（Concat/Zip）继承。
func mergeClosers(a, b []func() error) []func() error {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]func() error, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

// Cache 把一次性流 s 转为可重放工厂：首次调用工厂时求值 s 一次并物化
// 全部元素，此后每次调用返回全新的独立流（FromSlice 共享底层数组，
// 零拷贝）。
//
// 一次性模型保持：s 在首次调用时被消费（工厂从未被调用则 s 仍可用）；
// 工厂产物每次也是一次性流。
//
// 错误语义：物化期 s 出错 → 首错记忆进工厂，此后每次调用返回携带该
// 错误的空流（任何终止操作得空结果，Err() 返回该错误）。
func Cache[T any](s *Stream[T]) func() *Stream[T] {
	var once sync.Once
	var buf []T
	var err error
	return func() *Stream[T] {
		once.Do(func() {
			cs := &collectingSink[T]{limit: -1}
			s.pipeline.evaluate(cs)
			buf, err = cs.buf, s.pipeline.err
		})
		if err != nil {
			return emptyWithErr[T](err)
		}
		return FromSlice(buf)
	}
}

// emptyWithErr 构造携带既有错误的空流：求值时把错误注入错误槽
// （错误即值模型的源侧路径），任何终止操作返回空结果且 Err() 可查询。
func emptyWithErr[T any](err error) *Stream[T] {
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) {
			down.Begin(0)
			down.End()
			ec.fail(err)
		},
		chars: SpSized,
	}}
}
