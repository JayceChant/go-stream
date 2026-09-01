package stream

import (
	"errors"
	"testing"
)

// countingSplitterator 记录被推动次数的测试源：验证单遍融合与短路语义。
type countingSplitterator[T any] struct {
	testSplitterator[T]
	advanced int // 源被推动的总次数
}

func (c *countingSplitterator[T]) TryAdvance(f func(T) bool) bool {
	c.advanced++
	return c.testSplitterator.TryAdvance(f)
}

func (c *countingSplitterator[T]) ForEachRemaining(f func(T) bool) {
	for c.TryAdvance(f) {
	}
}

// fakeErrSource 经由错误短路路径驱动：手动构造带错误的求值（FromFunc 在 Task 3）。
// 这里通过 evalCtx.fail 直接模拟算子报错来测试引擎行为。
func newTestStream[T any](items ...T) *Stream[T] {
	return newHead(&testSplitterator[T]{
		baseSplitterator: baseSplitterator[T]{
			estSize: int64(len(items)),
			chars:   SpSized | SpOrdered | SpSubSized,
		},
		items: items,
	})
}

func TestEvaluateBasicFlow(t *testing.T) {
	// 基本流转：Begin/End 配对、元素全部送达
	var beginCnt, endCnt int
	var got []int
	term := &recordSink[int]{accept: func(v int) bool { got = append(got, v); return true }}
	term.onBegin = func(size int64) { beginCnt++; got = got[:0] }
	term.onEnd = func() { endCnt++ }

	s := newTestStream(1, 2, 3)
	ec := s.pipeline.evaluate(term)
	if ec.err != nil {
		t.Fatalf("unexpected err: %v", ec.err)
	}
	if beginCnt != 1 || endCnt != 1 {
		t.Errorf("Begin/End 调用 %d/%d 次, 期望各 1 次", beginCnt, endCnt)
	}
	if len(got) != 3 {
		t.Errorf("送达 %d 个元素, 期望 3", len(got))
	}
	if !s.pipeline.consumed {
		t.Error("求值后 consumed 应已置位")
	}
}

// recordSink 记录 Begin/End 调用并委托回调的测试终端 sink。
type recordSink[T any] struct {
	accept  func(T) bool
	onBegin func(int64)
	onEnd   func()
}

func (r *recordSink[T]) Begin(size int64) {
	if r.onBegin != nil {
		r.onBegin(size)
	}
}
func (r *recordSink[T]) Accept(v T) bool { return r.accept(v) }
func (r *recordSink[T]) End() {
	if r.onEnd != nil {
		r.onEnd()
	}
}

func TestStatelessFusionSinglePass(t *testing.T) {
	// 无状态链单遍融合：Filter+Map 链下源只被推动一次
	src := &countingSplitterator[int]{}
	src.items = []int{1, 2, 3, 4, 5}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}

	var mapped []int
	s := newHead(src)
	s2 := newStateless(s,
		func(down Sink[int], _ *evalCtx) Sink[int] {
			return &wrapFilter[int]{down: down, p: func(v int) bool { return v%2 == 0 }}
		},
		s.chars,
	)
	s3 := newStateless(s2,
		func(down Sink[int], _ *evalCtx) Sink[int] {
			return &wrapMap[int]{down: down, f: func(v int) int { return v * 10 }}
		},
		s2.chars,
	)
	s3.pipeline.evaluate(&recordSink[int]{accept: func(v int) bool {
		mapped = append(mapped, v)
		return true
	}})

	if want := []int{20, 40}; len(mapped) != 2 || mapped[0] != 20 || mapped[1] != 40 {
		t.Errorf("mapped = %v, 期望 %v", mapped, want)
	}
	if src.advanced != 6 {
		t.Errorf("源被推动 %d 次, 期望 6（5 个元素 + 1 次耗尽探测，单遍融合）", src.advanced)
	}
}

// wrapFilter / wrapMap 测试用无状态包装 sink（真实算子在 Task 4 实现）。
type wrapFilter[T any] struct {
	down Sink[T]
	p    func(T) bool
}

func (w *wrapFilter[T]) Begin(size int64) { w.down.Begin(-1) }
func (w *wrapFilter[T]) Accept(v T) bool {
	if w.p(v) {
		return w.down.Accept(v)
	}
	return true
}
func (w *wrapFilter[T]) End() { w.down.End() }

type wrapMap[T any] struct {
	down Sink[T]
	f    func(T) T
}

func (w *wrapMap[T]) Begin(size int64) { w.down.Begin(-1) }
func (w *wrapMap[T]) Accept(v T) bool  { return w.down.Accept(w.f(v)) }
func (w *wrapMap[T]) End()             { w.down.End() }

func TestStatefulSegmentedEvaluation(t *testing.T) {
	// 有状态分段：第一段物化 5 个，排序后续段单遍回放 5 个
	src := &countingSplitterator[int]{}
	src.items = []int{5, 3, 1, 4, 2}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}

	var got []int
	s2 := newStateful(newHead(src2Items()), -1,
		func(buf []int) []int {
			out := append([]int(nil), buf...)
			for i := 1; i < len(out); i++ {
				for j := i; j > 0 && out[j] < out[j-1]; j-- {
					out[j], out[j-1] = out[j-1], out[j]
				}
			}
			return out
		},
		SpOrdered|SpSubSized,
	)
	s2.pipeline.evaluate(&recordSink[int]{accept: func(v int) bool {
		got = append(got, v)
		return true
	}})
	if want := []int{1, 2, 3, 4, 5}; len(got) != 5 || got[0] != 1 || got[4] != 5 {
		t.Errorf("got = %v, 期望 %v", got, want)
	}
	if src.advanced != 0 {
		t.Error("第一个流未被求值不应消耗 src（惰性验证）")
	}
}

// src2Items 生成新源避免复用已消费源。
func src2Items() *countingSplitterator[int] {
	src := &countingSplitterator[int]{}
	src.items = []int{5, 3, 1, 4, 2}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}
	return src
}

func TestShortCircuit(t *testing.T) {
	// 短路：终端取 2 个即停，源只被推动 2 次
	src := &countingSplitterator[int]{}
	src.items = []int{1, 2, 3, 4, 5}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}

	count := 0
	newHead(src).pipeline.evaluate(&recordSink[int]{accept: func(v int) bool {
		count++
		return count < 2
	}})
	if count != 2 {
		t.Errorf("短路后消费 %d 个, 期望 2", count)
	}
	if src.advanced != 2 {
		t.Errorf("源被推动 %d 次, 期望 2（短路生效）", src.advanced)
	}
}

func TestStatefulLimitCollect(t *testing.T) {
	// 物化段 limit 截断：无限源的收集上限（Limit 算子的引擎基础）
	src := &countingSplitterator[int]{}
	src.items = []int{1, 2, 3, 4, 5}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}

	var got []int
	s := newStateful(newHead(src), 3, func(buf []int) []int { return buf }, SpOrdered|SpSubSized)
	s.pipeline.evaluate(&recordSink[int]{accept: func(v int) bool {
		got = append(got, v)
		return true
	}})
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("got = %v, 期望前 3 个 [1 2 3]", got)
	}
	if src.advanced != 4 {
		t.Errorf("源被推动 %d 次, 期望 4（3 个成功 + 1 次拒绝探测）", src.advanced)
	}
}

func TestOnceConsumedPanic(t *testing.T) {
	// 重复终止求值：第二次 panic
	s := newTestStream(1)
	s.pipeline.evaluate(&recordSink[int]{accept: func(int) bool { return true }})
	defer func() {
		if r := recover(); r == nil {
			t.Error("重复求值应 panic")
		} else if r != errConsumed {
			t.Errorf("panic 信息不符: %v", r)
		}
	}()
	s.pipeline.evaluate(&recordSink[int]{accept: func(int) bool { return true }})
}

func TestLinkConsumedPanic(t *testing.T) {
	// 重复链接：第二次 newStateless panic
	s := newTestStream(1)
	_ = newStateless(s, func(down Sink[int], _ *evalCtx) Sink[int] { return down }, s.chars)
	defer func() {
		if r := recover(); r == nil {
			t.Error("重复链接应 panic")
		}
	}()
	_ = newStateless(s, func(down Sink[int], _ *evalCtx) Sink[int] { return down }, s.chars)
}

func TestErrorShortCircuit(t *testing.T) {
	// 错误短路：算子经 evalCtx.fail 请求取消，源停止推动，End 仍被调用
	src := &countingSplitterator[int]{}
	src.items = []int{1, 2, 3, 4, 5}
	src.baseSplitterator = baseSplitterator[int]{estSize: 5, chars: SpSized | SpOrdered}

	boom := errors.New("boom")
	endCalled := false
	s := newHead(src)
	s2 := newStateless(s, func(down Sink[int], ec *evalCtx) Sink[int] {
		return &wrapMapErr[int]{down: down, ec: ec, f: func(v int) (int, error) {
			if v == 2 {
				return 0, boom
			}
			return v, nil
		}}
	}, s.chars)
	ec := s2.pipeline.evaluate(&recordSink[int]{
		accept: func(int) bool { return true },
		onEnd:  func() { endCalled = true },
	})
	if !errors.Is(ec.err, boom) {
		t.Errorf("ec.err = %v, 期望 boom", ec.err)
	}
	if !errors.Is(s2.pipeline.err, boom) {
		t.Errorf("Stream.err 未写回首错: %v", s2.pipeline.err)
	}
	if src.advanced != 2 {
		t.Errorf("源被推动 %d 次, 期望 2（错误短路）", src.advanced)
	}
	if !endCalled {
		t.Error("错误路径 End 仍应被调用（部分结果语义）")
	}
}

// wrapMapErr 测试用带错误的映射包装 sink（真实 MapErr 在 Task 4）。
type wrapMapErr[T any] struct {
	down Sink[T]
	ec   *evalCtx
	f    func(T) (T, error)
}

func (w *wrapMapErr[T]) Begin(size int64) { w.down.Begin(-1) }
func (w *wrapMapErr[T]) Accept(v T) bool {
	out, err := w.f(v)
	if err != nil {
		return w.ec.fail(err) // 记录首错并短路
	}
	return w.down.Accept(out)
}
func (w *wrapMapErr[T]) End() { w.down.End() }
