package stream

import "iter"

// sources.go：各数据源的 Splitterator 实现（全部嵌入 baseSplitterator 基座）。
//
// 可分裂源（sliceSp/rangeSp）实现二分 TrySplit：返回后半段新源、自身收缩为
// 前半段，两段不重叠且并集等于原集合（有序语义下保持相遇顺序）——为后续
// 并行求值（TODO）提供均衡拆分能力。生成器型源（seq/channel/func）不可分裂。

// sliceSp 是 slice 数据源的 Splitterator（零拷贝：直接引用原切片区间）。
type sliceSp[T any] struct {
	baseSplitterator[T]
	s []T
	i int // 下一个待读下标
}

// 编译期检查：sliceSp 实现 Splitterator。
var _ Splitterator[int] = (*sliceSp[int])(nil)

// newSliceSp 基于（子）切片构造源：estSize 为剩余元素数。
func newSliceSp[T any](s []T, chars Characteristics) *sliceSp[T] {
	if chars&SpSized != 0 {
		chars |= SpSubSized
	}
	return &sliceSp[T]{baseSplitterator[T]{int64(len(s)), chars}, s, 0}
}

func (sp *sliceSp[T]) TryAdvance(f func(T) bool) bool {
	if sp.i >= len(sp.s) {
		return false
	}
	v := sp.s[sp.i]
	sp.i++
	return f(v)
}

func (sp *sliceSp[T]) ForEachRemaining(f func(T) bool) {
	for sp.i < len(sp.s) {
		v := sp.s[sp.i]
		sp.i++
		if !f(v) {
			return
		}
	}
}

// TrySplit 二分剩余区间：返回后半段，自身收缩为前半段；元素少于 2 个不分裂。
func (sp *sliceSp[T]) TrySplit() Splitterator[T] {
	rest := sp.s[sp.i:]
	if len(rest) < 2 {
		return nil
	}
	mid := len(rest) / 2
	front, back := rest[:mid], rest[mid:]
	sp.s = front
	sp.baseSplitterator.estSize = int64(len(front))
	return &sliceSp[T]{baseSplitterator[T]{int64(len(back)), sp.baseSplitterator.chars}, back, 0}
}

// rangeSp 是整数区间源的 Splitterator（左闭右开 [cur, stop)，步长 1）。
type rangeSp[I Integer] struct {
	baseSplitterator[I]
	cur  I
	stop I
}

// 编译期检查：rangeSp 实现 Splitterator。
var _ Splitterator[int] = (*rangeSp[int])(nil)

func newRangeSp[I Integer](cur, stop I, chars Characteristics) *rangeSp[I] {
	if chars&SpSized != 0 {
		chars |= SpSubSized // 子区间同样可精确报告大小（与 newSliceSp 对齐，Task 11 修正）
	}
	var n int64
	if cur < stop {
		n = int64(stop - cur)
	} else {
		n = 0
	}
	return &rangeSp[I]{baseSplitterator[I]{n, chars}, cur, stop}
}

func (sp *rangeSp[I]) TryAdvance(f func(I) bool) bool {
	if sp.cur >= sp.stop {
		return false
	}
	v := sp.cur
	sp.cur++
	return f(v)
}

func (sp *rangeSp[I]) ForEachRemaining(f func(I) bool) {
	for sp.cur < sp.stop {
		v := sp.cur
		sp.cur++
		if !f(v) {
			return
		}
	}
}

// TrySplit 二分剩余区间（溢出安全：以中点公式计算，避免 (cur+stop) 溢出）。
func (sp *rangeSp[I]) TrySplit() Splitterator[I] {
	if sp.cur >= sp.stop || sp.stop-sp.cur < 2 {
		return nil
	}
	mid := sp.cur + (sp.stop-sp.cur)/2
	back := &rangeSp[I]{baseSplitterator[I]{int64(sp.stop - mid), sp.baseSplitterator.chars}, mid, sp.stop}
	sp.baseSplitterator.estSize = int64(mid - sp.cur)
	sp.stop = mid
	return back
}

// seqSp 是 iter.Seq 源（push 模型，不可分裂，一次性：遍历即消耗）。
// 经 iter.Pull 转为拉取式，从而正确支持 TryAdvance 的单步推进语义；
// 消费方请求取消（f 返回 false）时丢弃剩余元素（push 迭代器无法暂停）。
type seqSp[T any] struct {
	baseSplitterator[T]
	next func() (T, bool)
	stop func()
	done bool
}

func newSeqSp[T any](seq iter.Seq[T]) *seqSp[T] {
	next, stop := iter.Pull(seq)
	return &seqSp[T]{baseSplitterator[T]{-1, SpOrdered}, next, stop, false}
}

func (sp *seqSp[T]) TryAdvance(f func(T) bool) bool {
	if sp.done {
		return false
	}
	v, ok := sp.next()
	if !ok {
		sp.done = true
		return false
	}
	if !f(v) {
		sp.done = true
		sp.stop() // 取消：释放底层迭代器
		return false
	}
	return true
}

func (sp *seqSp[T]) ForEachRemaining(f func(T) bool) {
	for sp.TryAdvance(f) {
	}
}

// channelSp 是 channel 源（阻塞拉取，不可分裂；通道耗尽即结束）。
type channelSp[T any] struct {
	baseSplitterator[T]
	ch <-chan T
}

func newChannelSp[T any](ch <-chan T) *channelSp[T] {
	return &channelSp[T]{baseSplitterator[T]{-1, SpOrdered}, ch}
}

func (sp *channelSp[T]) TryAdvance(f func(T) bool) bool {
	v, ok := <-sp.ch
	if !ok {
		return false
	}
	return f(v)
}

func (sp *channelSp[T]) ForEachRemaining(f func(T) bool) {
	for v := range sp.ch {
		if !f(v) {
			return
		}
	}
}

// funcSp 是拉式函数源：next 返回 (元素, 是否还有, 错误)。
// 适配 IO/解析场景；错误经 evalCtx 以错误即值模型传播（见 FromFunc）。
type funcSp[T any] struct {
	baseSplitterator[T]
	next func() (T, bool, error)
	err  error // 首错缓存
}

// 编译期检查：funcSp 实现 Splitterator。
var _ Splitterator[int] = (*funcSp[int])(nil)

func newFuncSp[T any](next func() (T, bool, error)) *funcSp[T] {
	return &funcSp[T]{baseSplitterator[T]{-1, SpOrdered}, next, nil}
}

func (sp *funcSp[T]) TryAdvance(f func(T) bool) bool {
	if sp.err != nil {
		return false
	}
	v, ok, err := sp.next()
	if err != nil {
		sp.err = err
		return false
	}
	if !ok {
		return false
	}
	return f(v)
}

func (sp *funcSp[T]) ForEachRemaining(f func(T) bool) {
	for sp.TryAdvance(f) {
	}
}

// FirstErr 返回源已发生的首错（供 FromFunc 专属 head 求值路径读取）。
func (sp *funcSp[T]) FirstErr() error { return sp.err }
