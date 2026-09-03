package stream

import "slices"

// ops_stateful.go：有状态中间操作。
//
// 分两类：
//   - 物化型（newStateful）：Limit/Skip/SpSorted/DistinctBy/Reverse——求值时先
//     驱动上游段物化进缓冲（limit 可截断），再对缓冲做纯切片变换后单遍回放
//   - 单遍型（newStateless + 自维护状态）：Scan/Chunk/Enumerate——有状态但
//     无需物化，元素边到边走（支持无限源）
//
// 物化后特征位统一规则：SpOrdered 保留；SpSized 保留（缓冲长度已知）并置
// SpSubSized；SpSorted/SpDistinct 由各算子按语义设置。

// Limit 截取前 n 个元素（n == 0 得空流；无限源可借此终止；n < 0 panic）。
func (s *Stream[T]) Limit(n int64) *Stream[T] {
	if n < 0 {
		panic("stream: Limit 参数为负")
	}
	return newStateful(s, n, func(buf []T) []T { return buf },
		(s.chars|SpSized|SpSubSized)&^SpSorted)
}

// Skip 跳过前 n 个元素，输出其余（n == 0 恒等返回原流，不物化、特征位透传；
// n < 0 panic）。恒等返回时不标记上游 consumed，原流仍可继续链接。
func (s *Stream[T]) Skip(n int64) *Stream[T] {
	if n < 0 {
		panic("stream: Skip 参数为负")
	}
	if n == 0 { // no-op 特例：免物化/免降级，语义同 JDK skip(0) returns this
		return s
	}
	return newStateful(s, -1, func(buf []T) []T {
		if int64(len(buf)) <= n {
			return nil
		}
		return buf[n:]
	}, s.chars|SpSized|SpSubSized)
}

// Sorted 按比较器 cmp 升序稳定排序（cmp 负/零/正 表示小于/等于/大于）。
// 就地排序物化缓冲：collectingSink 物化的缓冲为本次求值独占的全新切片
// （append 构建，非源切片别名），不克隆即排序，省一次全量拷贝；
// 用户源切片不受影响（回归测试 TestSortedStable 守护）。
func (s *Stream[T]) Sorted(cmp func(a, b T) int) *Stream[T] {
	if cmp == nil {
		panic("stream: Sorted 比较器为 nil")
	}
	return newStateful(s, -1, func(buf []T) []T {
		slices.SortStableFunc(buf, cmp)
		return buf
	}, (s.chars|SpSorted)&^SpDistinct)
}

// DistinctBy 依据 key 函数去重：每组同 key 仅保留首个遇到的元素（保遇序）。
// K 须满足 comparable：键为具体不可比较类型（slice/map/func 等）时编译期即报错。
// 逃生口：K 显式取 any（接口满足 comparable）仍可编译，动态类型不可比较时
// 在求值时 panic（用户契约，同 map 键语义）。
func (s *Stream[T]) DistinctBy[K comparable](key func(T) K) *Stream[T] {
	if key == nil {
		panic("stream: DistinctBy 键函数为 nil")
	}
	return newStateful(s, -1, func(buf []T) []T {
		seen := make(map[K]struct{}, len(buf))
		out := make([]T, 0, len(buf))
		for _, v := range buf {
			k := key(v)
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				out = append(out, v)
			}
		}
		return out
	}, (s.chars|SpDistinct)&^SpSorted)
}

// Reverse 反转元素顺序。
// 就地反转物化缓冲（独占切片，非源别名，同 Sorted 的论证）。
func (s *Stream[T]) Reverse() *Stream[T] {
	return newStateful(s, -1, func(buf []T) []T {
		slices.Reverse(buf)
		return buf
	}, s.chars)
}

// ---- 单遍型有状态算子（不物化，支持无限源）----

// Scan 滚动累积（前缀和式）：输出 seed, f(seed,x1), f(f(seed,x1),x2), ...
// 输出个数与输入相同（含初值，比 Java 无对应物的常见 Go 实现多一项）。
// 有状态单遍（滚动 acc）→ 并行降级（splitN=nil）。
func (s *Stream[T]) Scan[U any](seed U, f func(U, T) U) *Stream[U] {
	if f == nil {
		panic("stream: Scan 函数为 nil")
	}
	ns := newStateless(s, func(down Sink[U], _ *evalCtx) Sink[T] {
		return &scanSink[T, U]{down: down, acc: seed, f: f}
	}, s.chars&^(SpSized|SpDistinct|SpSorted))
	ns.splitN = nil
	return ns
}

type scanSink[T, U any] struct {
	down    Sink[U]
	acc     U
	f       func(U, T) U
	started bool
}

func (w *scanSink[T, U]) Begin(int64) { w.down.Begin(-1) }
func (w *scanSink[T, U]) Accept(v T) bool {
	if !w.started { // 先输出种子
		w.started = true
		if !w.down.Accept(w.acc) {
			return false
		}
	}
	w.acc = w.f(w.acc, v)
	return w.down.Accept(w.acc)
}
func (w *scanSink[T, U]) End() { w.down.End() }

// chunkSink 与 enumerateSink 的包级入口见 op_ext.go（Chunk/Enumerate）。
// Go 1.27 限制：泛型方法返回"由 T 派生的类型"（Stream[[]T]、Stream[KV[int,T]]）
// 会触发实例化循环（T → []T → [][]T → ...），故二者为包级函数形态。

type chunkSink[T any] struct {
	down Sink[[]T]
	n    int
	cur  []T
}

func (w *chunkSink[T]) Begin(int64) {
	w.down.Begin(-1)
	w.cur = make([]T, 0, w.n)
}

func (w *chunkSink[T]) Accept(v T) bool {
	w.cur = append(w.cur, v)
	if len(w.cur) == w.n {
		full := w.cur
		w.cur = make([]T, 0, w.n)
		return w.down.Accept(full)
	}
	return true
}

func (w *chunkSink[T]) End() {
	if len(w.cur) > 0 { // 尾组
		w.down.Accept(w.cur)
	}
	w.down.End()
}

type enumerateSink[T any] struct {
	down Sink[KV[int, T]]
	i    int
}

func (w *enumerateSink[T]) Begin(size int64) { w.down.Begin(size) }
func (w *enumerateSink[T]) Accept(v T) bool {
	kv := KV[int, T]{Key: w.i, Value: v}
	w.i++
	return w.down.Accept(kv)
}
func (w *enumerateSink[T]) End() { w.down.End() }
