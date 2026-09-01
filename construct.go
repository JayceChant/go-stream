package stream

import "iter"

// construct.go：包级流构造函数（全部惰性：构造不触发任何遍历）。
//
// 一次性语义：每个构造出的 *Stream 仅可被链接或消费一次（重复使用 panic）。
// FromSlice/Of 直接引用原 slice 不拷贝，求值期间请勿并发修改。

// Of 以可变参数构建流。
func Of[T any](xs ...T) *Stream[T] {
	return newHeadSplit(newSliceSp(xs, SpSized|SpOrdered))
}

// Empty 构建空流。
func Empty[T any]() *Stream[T] {
	return newHead(newSliceSp[T](nil, SpSized|SpOrdered))
}

// FromSlice 基于 slice 构建流（零拷贝，直接引用原切片）。
func FromSlice[T any](s []T) *Stream[T] {
	return newHeadSplit(newSliceSp(s, SpSized|SpOrdered))
}

// FromSeq 基于 Go 1.23 push 迭代器构建流（一次性：seq 无法暂停复用）。
func FromSeq[T any](seq iter.Seq[T]) *Stream[T] {
	if seq == nil {
		return Empty[T]()
	}
	return newHead(newSeqSp(seq))
}

// FromChannel 基于 channel 构建流（阻塞拉取直到通道关闭）。
func FromChannel[T any](ch <-chan T) *Stream[T] {
	return newHead(newChannelSp(ch))
}

// FromMap 基于 map 构建流，产出 KV 键值对元素。
// map 遍历顺序不确定，故本源为 Unordered（不声明 SpOrdered——
// Task 10 修正：此前经 newSeqSp 误置 SpOrdered，与本源语义矛盾）。
func FromMap[K comparable, V any](m map[K]V) *Stream[KV[K, V]] {
	sp := newSeqSp(iter.Seq[KV[K, V]](func(yield func(KV[K, V]) bool) {
		for k, v := range m {
			if !yield(KV[K, V]{Key: k, Value: v}) {
				return
			}
		}
	}))
	sp.baseSplitterator.chars &^= SpOrdered // 遍历序不确定：Unordered
	return newHead(sp)
}

// FromFunc 基于拉式函数构建流：next 返回 (元素, 是否还有, 错误)。
// 适配 IO/解析等可失败源；首错经 Err() 查询，出错时保留已产出的部分结果。
func FromFunc[T any](next func() (T, bool, error)) *Stream[T] {
	if next == nil {
		return Empty[T]()
	}
	sp := newFuncSp(next)
	return &Stream[T]{pipeline[T]{
		drive:  driveFuncErr(sp),
		source: sp,
		chars:  sp.Characteristics(),
	}}
}

// driveFuncErr 编译 funcSp 源的 Head 段求值闭包：
// 遍历结束后把源首错转入 evalCtx（与算子错误同路，供 Err() 读取）。
func driveFuncErr[T any](sp *funcSp[T]) driveFunc[T] {
	return func(down Sink[T], ec *evalCtx) {
		down.Begin(sp.EstimateSize())
		sp.ForEachRemaining(func(t T) bool { return down.Accept(t) })
		down.End()
		if err := sp.FirstErr(); err != nil {
			ec.fail(err)
		}
	}
}

// Generate 以无限生成函数构建流（须配合 Limit 等短路算子终止）。
func Generate[T any](f func() T) *Stream[T] {
	if f == nil {
		return Empty[T]()
	}
	return FromFunc(func() (T, bool, error) {
		return f(), true, nil
	})
}

// Iterate 以种子与后继函数构建无限流：seed, f(seed), f(f(seed)), ...。
func Iterate[T any](seed T, next func(T) T) *Stream[T] {
	if next == nil {
		return Empty[T]()
	}
	cur := seed
	return FromFunc(func() (T, bool, error) {
		v := cur
		cur = next(cur)
		return v, true, nil
	})
}

// Range 构建整数区间流 [start, stop)（左闭右开，步长 1）。
func Range[I Integer](start, stop I) *Stream[I] {
	return newHeadSplit(newRangeSp(start, stop, SpSized|SpOrdered))
}

// newHeadSplit 构造可分源的 Head：额外设置 splitN 分片闭包
// （并行求值入口，见 parallel.go）。
func newHeadSplit[T any](src Splitterator[T]) *Stream[T] {
	s := newHead(src)
	s.splitN = splitNOf(src)
	return s
}

// Concat 串联两条流：先耗尽 a 再消费 b（a、b 均被标记消费）。
// 两段推入同一下游 sink，共用一次 Begin/End（经 suppressEnd 吞掉 a 段
// 下传的 End，由 b 段统一收尾；b 段自带的 Begin 被 skipBegin 吞掉）。
func Concat[T any](a, b *Stream[T]) *Stream[T] {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	a.checkLinked()
	b.checkLinked()
	driveA, driveB := a.drive, b.drive
	chars := (a.chars | b.chars) & ^SpSized // 长度不再精确
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) {
			driveA(suppressEnd[T]{down}, ec) // a 段：Begin 下传、End 吞掉
			if ec.firstErr() == nil {
				driveB(skipBegin[T]{down}, ec) // b 段：Begin 吞掉、End 下传
			} else {
				down.End() // 错误路径也保证 End 收尾
			}
		},
		chars: chars,
		// Concat 双源拼接不参与并行分片（splitN 降级为 nil）
		closers: mergeClosers(a.closers, b.closers), // 双方回调链按 a 先 b 后继承
	}}
}

// suppressEnd 吞掉下游 End 的包装（Concat 中 a 段结束后由 b 段收尾）。
type suppressEnd[T any] struct{ down Sink[T] }

func (s suppressEnd[T]) Begin(size int64) { s.down.Begin(size) }
func (s suppressEnd[T]) Accept(v T) bool  { return s.down.Accept(v) }
func (s suppressEnd[T]) End()             {}

// skipBegin 吞掉下游 Begin 的包装（Concat 中 b 段与 a 段共用一次 Begin）。
type skipBegin[T any] struct{ down Sink[T] }

func (s skipBegin[T]) Begin(int64)     {}
func (s skipBegin[T]) Accept(v T) bool { return s.down.Accept(v) }
func (s skipBegin[T]) End()            { s.down.End() }
