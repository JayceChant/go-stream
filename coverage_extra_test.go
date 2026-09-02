package stream

import (
	"errors"
	"slices"
	"testing"
)

// coverage_extra_test.go：随覆盖率提升目标补充的测试。
// 包含三类：nil 参数 panic 矩阵（编程错误契约）、各源 TryAdvance 边界分支、
// Err 变体与物化回放短路等既有测试清单未覆盖的路径。

// ---- nil 参数 panic 矩阵 ----

func TestNilArgumentPanics(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"ForEach", func() { Of(1).ForEach(nil) }},
		{"ForEachUntil", func() { Of(1).ForEachUntil(nil) }},
		{"Reduce", func() { Of(1).Reduce(0, nil) }},
		{"ReduceOpt", func() { Of(1).ReduceOpt(nil) }},
		{"FindAny", func() { Of(1).FindAny(nil) }},
		{"AnyMatch", func() { Of(1).AnyMatch(nil) }},
		{"AllMatch", func() { Of(1).AllMatch(nil) }},
		{"NoneMatch", func() { Of(1).NoneMatch(nil) }},
		{"Min-cmp", func() { Of(1).Min(nil) }},
		{"Max-cmp", func() { Of(1).Max(nil) }},
		{"FlatMapSeq", func() { Of(1).FlatMapSeq[int](nil) }},
		{"FilterErr", func() { Of(1).FilterErr(nil) }},
		{"FlatMapErr", func() { Of(1).FlatMapErr[int](nil) }},
		{"PeekErr", func() { Of(1).PeekErr(nil) }},
		{"Skip-negative", func() { Of(1).Skip(-1) }},
		{"Sorted", func() { Of(1).Sorted(nil) }},
		{"DistinctBy", func() { Of(1).DistinctBy[int](nil) }},
		{"Scan", func() { Of(1).Scan(0, nil) }},
		{"Zip-other", func() { Of(1).Zip[int, int](nil, nil) }},
		{"Zip-f", func() { Of(1).Zip[int, int](Of(1), nil) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("应 panic（参数为 nil）")
				}
			}()
			c.fn()
		})
	}
}

// ---- 构造函数 nil 与便捷函数边界 ----

func TestNumericNilAndEmptyCases(t *testing.T) {
	// Avg 空流：分母保护，返回 0。
	if got := Avg(Of[int]()); got != 0 {
		t.Errorf("Avg(空流) = %v, 期望 0", got)
	}
	// Contains nil 流：不求值直接 false。
	if Contains[int](nil, 1) {
		t.Error("Contains(nil) 应为 false")
	}
	// 免写比较器族对 nil 流：nil / 零值 + false。
	if Sorted[int](nil) != nil {
		t.Error("Sorted(nil) 应为 nil")
	}
	if v, ok := Min[int](nil); ok || v != 0 {
		t.Error("Min(nil) 应为 (0, false)")
	}
	if v, ok := Max[int](nil); ok || v != 0 {
		t.Error("Max(nil) 应为 (0, false)")
	}
	if Distinct[int](nil) != nil {
		t.Error("Distinct(nil) 应为 nil")
	}
}

func TestConstructNilCases(t *testing.T) {
	// FromSeq(nil) 等价空流。
	if n := FromSeq[int](nil).Count(); n != 0 {
		t.Errorf("FromSeq(nil).Count() = %d, 期望 0", n)
	}
	// Concat 单侧 nil：直接返回另一侧。
	if got := Concat[int](nil, Of(1, 2)).ToSlice(); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("Concat(nil, b) = %v, 期望 [1 2]", got)
	}
	if got := Concat(Of(3), nil).ToSlice(); !slices.Equal(got, []int{3}) {
		t.Errorf("Concat(a, nil) = %v, 期望 [3]", got)
	}
	// FromMap 下游短路（First）：覆盖 yield 取消提前退出分支。
	if _, ok := FromMap(map[string]int{"a": 1, "b": 2}).First(); !ok {
		t.Error("FromMap.First 应取到元素")
	}
}

// ---- 源级 TryAdvance 边界（白盒）----

func TestSliceSpTryAdvanceBoundaries(t *testing.T) {
	sp := newSliceSp([]int{1, 2, 3}, SpSized|SpOrdered)
	if !sp.TryAdvance(func(int) bool { return true }) {
		t.Error("首个元素 TryAdvance 应返回 true")
	}
	// 取消：f 返回 false，源推进到下一元素。
	if sp.TryAdvance(func(int) bool { return false }) {
		t.Error("取消时 TryAdvance 应返回 false")
	}
	var got []int
	sp.ForEachRemaining(func(v int) bool { got = append(got, v); return true })
	if !slices.Equal(got, []int{3}) {
		t.Errorf("剩余元素 = %v, 期望 [3]", got)
	}
	// 耗尽后恒 false。
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("耗尽后 TryAdvance 应返回 false")
	}
}

func TestRangeSpTryAdvanceBoundaries(t *testing.T) {
	sp := newRangeSp(0, 3, SpSized|SpOrdered)
	if !sp.TryAdvance(func(int) bool { return true }) {
		t.Error("首个元素 TryAdvance 应返回 true")
	}
	if sp.TryAdvance(func(int) bool { return false }) {
		t.Error("取消时 TryAdvance 应返回 false")
	}
	var got []int
	sp.ForEachRemaining(func(v int) bool { got = append(got, v); return true })
	if !slices.Equal(got, []int{2}) {
		t.Errorf("剩余元素 = %v, 期望 [2]", got)
	}
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("耗尽后 TryAdvance 应返回 false")
	}
}

func TestRangeSpTrySplitUnsplittable(t *testing.T) {
	// 空区间与单元素区间不可分裂。
	if newRangeSp(5, 5, SpSized).TrySplit() != nil {
		t.Error("空区间 TrySplit 应为 nil")
	}
	if newRangeSp(0, 1, SpSized).TrySplit() != nil {
		t.Error("单元素区间 TrySplit 应为 nil")
	}
}

func TestSeqSpTryAdvanceAfterDone(t *testing.T) {
	// 耗尽置 done 后再次 TryAdvance：首次走耗尽分支，其后走 done 早退恒 false。
	sp := newSeqSp(seqIntsOf1())
	if !sp.TryAdvance(func(int) bool { return true }) {
		t.Fatal("唯一元素 TryAdvance 应返回 true")
	}
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("耗尽后 TryAdvance 应返回 false")
	}
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("done 早退后 TryAdvance 仍应返回 false")
	}
}

func TestChannelSpExhaustedAndForEachStop(t *testing.T) {
	// 通道关闭后 TryAdvance 返回 false。
	ch := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		ch <- i
	}
	close(ch)
	sp := newChannelSp(ch)
	for sp.TryAdvance(func(int) bool { return true }) {
	}
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("关闭通道 TryAdvance 应返回 false")
	}
	// ForEachRemaining 取消：f 返回 false 提前停止，剩余元素留在通道。
	ch2 := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		ch2 <- i
	}
	n := 0
	newChannelSp(ch2).ForEachRemaining(func(int) bool { n++; return n < 2 })
	if n != 2 || len(ch2) != 2 {
		t.Errorf("取消后 f 调用 %d 次、通道剩 %d, 期望 2/2", n, len(ch2))
	}
}

// ---- 短路穿透有状态/单遍算子的回放与种子路径 ----

func TestStatefulReplayShortCircuit(t *testing.T) {
	// 终端 First 在物化回放阶段短路：命中 newStateful 回放循环的取消分支。
	if v, ok := Of(3, 1, 2).Sorted(func(a, b int) int { return a - b }).First(); !ok || v != 1 {
		t.Errorf("Sorted 后 First = (%d, %v), 期望 (1, true)", v, ok)
	}
}

func TestScanSeedShortCircuit(t *testing.T) {
	// First 在 Scan 种子输出后即取消：命中 scanSink 种子短路分支。
	if v, ok := Of(5, 6).Scan(0, func(a, v int) int { return a + v }).First(); !ok || v != 0 {
		t.Errorf("Scan 种子短路 First = (%d, %v), 期望 (0, true)", v, ok)
	}
}

func TestFlatMapDownstreamShortCircuit(t *testing.T) {
	// 展开中途下游取消：命中 flatMapSink 展开循环的取消分支。
	if v, ok := Of(1, 2).FlatMap(func(v int) []int { return []int{v, v * 10} }).First(); !ok || v != 1 {
		t.Errorf("FlatMap 展开短路 First = (%d, %v), 期望 (1, true)", v, ok)
	}
}

// ---- Err 变体路径（首错短路 + 部分结果）----

func TestErrVariantPaths(t *testing.T) {
	// FilterErr：谓词出错记首错并短路，保留此前部分结果。
	fe := errors.New("filter-err")
	s := Of(1, 2, 3, 4).FilterErr(func(v int) (bool, error) {
		if v == 4 {
			return false, fe
		}
		return v%2 == 0, nil // 过滤掉奇数（谓词 false 不短路）
	})
	if got := s.ToSlice(); !slices.Equal(got, []int{2}) {
		t.Errorf("FilterErr 部分结果 = %v, 期望 [2]", got)
	}
	if !errors.Is(s.Err(), fe) {
		t.Errorf("FilterErr Err() = %v, 期望 %v", s.Err(), fe)
	}

	// FlatMapErr：出错短路。
	me := errors.New("flatmap-err")
	s2 := Of(1).FlatMapErr(func(int) ([]int, error) { return nil, me })
	if got := s2.ToSlice(); len(got) != 0 {
		t.Errorf("FlatMapErr 出错结果 = %v, 期望空", got)
	}
	if !errors.Is(s2.Err(), me) {
		t.Errorf("FlatMapErr Err() = %v, 期望 %v", s2.Err(), me)
	}

	// FlatMapErr：展开中途下游取消。
	if v, ok := Of(1).FlatMapErr(func(int) ([]int, error) { return []int{1, 2}, nil }).First(); !ok || v != 1 {
		t.Errorf("FlatMapErr 展开短路 First = (%d, %v), 期望 (1, true)", v, ok)
	}

	// FlatMapErr：正常展开耗尽（下游不取消，循环自然完成）。
	if got := Of(1, 2).FlatMapErr(func(v int) ([]int, error) { return []int{v, v * 10}, nil }).ToSlice(); !slices.Equal(got, []int{1, 10, 2, 20}) {
		t.Errorf("FlatMapErr 正常展开 = %v, 期望 [1 10 2 20]", got)
	}

	// PeekErr：副作用出错记首错并短路。
	pe := errors.New("peek-err")
	s3 := Of(1, 2).PeekErr(func(v int) error {
		if v == 2 {
			return pe
		}
		return nil
	})
	if got := s3.ToSlice(); !slices.Equal(got, []int{1}) {
		t.Errorf("PeekErr 部分结果 = %v, 期望 [1]", got)
	}
	if !errors.Is(s3.Err(), pe) {
		t.Errorf("PeekErr Err() = %v, 期望 %v", s3.Err(), pe)
	}
}

// ---- 并行与收集器 ----

func TestParallelNonPositiveN(t *testing.T) {
	// n<=1 收敛为 1：结果正确，不 panic。
	for _, n := range []int{0, -2} {
		if got := FromSlice([]int{1, 2, 3}).Parallel(n).ToSlice(); !slices.Equal(got, []int{1, 2, 3}) {
			t.Errorf("Parallel(%d) = %v, 期望 [1 2 3]", n, got)
		}
	}
}

func TestSummingParallelCollect(t *testing.T) {
	// Summing 的 Combiner 在并行 Collect 下按片合并（此前仅串行被测）。
	data := make([]int, 100)
	for i := range data {
		data[i] = 1
	}
	if got := FromSlice(data).Parallel(4).Collect(Summing[int]()); got != 100 {
		t.Errorf("并行 Summing = %d, 期望 100", got)
	}
}

func TestConcatMergeClosersOneSideEmpty(t *testing.T) {
	// a 侧带回调、b 侧无：合并链保留 a 侧（mergeClosers 直接返回 a），
	// 终止求值后触发。
	called := false
	s := Concat(Of(1).OnClose(func() error { called = true; return nil }), Of(2))
	if got := s.ToSlice(); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("Concat = %v, 期望 [1 2]", got)
	}
	if !called {
		t.Error("终止求值后 a 侧 OnClose 回调未触发")
	}
}

// ---- sliceTotal 类型容错与取消分支（白盒）----

func TestSliceTotalNonCollectingPartSkipped(t *testing.T) {
	// total/pushPart 跳过非 collectingSink 的片（类型断言失败容错），
	// total 仍保证 Begin/End 配对。
	rs := &recordSink[int]{accept: func(int) bool { return true }}
	st := sliceTotal[int]{}
	st.total([]Sink[int]{&countSink[int]{}}, rs, &evalCtx{})
	if !st.pushPart(0, []Sink[int]{&countSink[int]{}}, rs, &evalCtx{}) {
		t.Error("pushPart 非 collectingSink 应返回 true")
	}
}

func TestSliceTotalPushPartCancel(t *testing.T) {
	// 流式合并回放时下游取消：pushPart 返回 false。
	cs := &collectingSink[int]{buf: []int{1, 2}}
	st := sliceTotal[int]{}
	if st.pushPart(0, []Sink[int]{cs}, &recordSink[int]{
		accept: func(int) bool { return false },
	}, &evalCtx{}) {
		t.Error("下游取消时 pushPart 应返回 false")
	}
}
