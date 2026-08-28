package stream

import (
	"errors"
	"testing"
)

func TestFromSliceAndOf(t *testing.T) {
	// 基本流转：元素顺序与数量
	got := collectViaProbe(FromSlice([]int{3, 1, 2}))
	if len(got) != 3 || got[0] != 3 || got[1] != 1 || got[2] != 2 {
		t.Errorf("got = %v, 期望 [3 1 2]", got)
	}
	// 空流
	if got := collectViaProbe(Empty[int]()); len(got) != 0 {
		t.Errorf("空流应无元素, got %v", got)
	}
	// Of
	if got := collectViaProbe(Of("a", "b")); len(got) != 2 || got[1] != "b" {
		t.Errorf("Of 结果异常: %v", got)
	}
}

func TestRange(t *testing.T) {
	got := collectViaProbe(Range(0, 5))
	want := []int{0, 1, 2, 3, 4}
	if len(got) != 5 {
		t.Fatalf("Range(0,5) 产出 %d 个, 期望 5", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, 期望 %d", i, got[i], want[i])
		}
	}
	// 空区间与反向区间
	if got := collectViaProbe(Range(5, 5)); len(got) != 0 {
		t.Error("空区间应无元素")
	}
	if got := collectViaProbe(Range(5, 0)); len(got) != 0 {
		t.Error("反向区间应无元素")
	}
}

func TestGenerateAndIterate(t *testing.T) {
	// Generate + 引擎 limit 截断（无限源必须可短路终止，不可直接求值）
	n := 0
	s := newStateful(Generate(func() int { n++; return n }), 4,
		func(buf []int) []int { return buf }, Ordered|SubSized)
	got := collectViaProbe(s)
	if len(got) != 4 || got[3] != 4 {
		t.Errorf("Generate 截断结果 = %v, 期望 [1 2 3 4]", got)
	}
	if n != 4 {
		t.Errorf("生成函数调用 %d 次, 期望 4（短路收集）", n)
	}

	// Iterate + newStateful(limit) 截断
	s2 := newStateful(Iterate(1, func(v int) int { return v * 2 }), 4,
		func(buf []int) []int { return buf }, Ordered|SubSized)
	got2 := collectViaProbe(s2)
	if len(got2) != 4 || got2[3] != 8 {
		t.Errorf("Iterate 截断结果 = %v, 期望 [1 2 4 8]", got2)
	}
}

func TestFromSeq(t *testing.T) {
	seq := func(yield func(int) bool) {
		for i := 1; i <= 3; i++ {
			if !yield(i) {
				return
			}
		}
	}
	got := collectViaProbe(FromSeq(seq))
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("FromSeq 结果 = %v, 期望 [1 2 3]", got)
	}
}

func TestFromChannel(t *testing.T) {
	ch := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		ch <- i
	}
	close(ch)
	got := collectViaProbe(FromChannel(ch))
	if len(got) != 3 || got[2] != 3 {
		t.Errorf("FromChannel 结果 = %v, 期望 [1 2 3]", got)
	}
}

func TestFromMap(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	got := collectViaProbe(FromMap(m))
	if len(got) != 3 {
		t.Fatalf("FromMap 产出 %d 个, 期望 3", len(got))
	}
	byKey := map[string]int{}
	for _, kv := range got {
		byKey[kv.Key] = kv.Value
	}
	for k, v := range m {
		if byKey[k] != v {
			t.Errorf("KV(%s) = %d, 期望 %d", k, byKey[k], v)
		}
	}
}

func TestFromFuncError(t *testing.T) {
	boom := errors.New("io failure")
	calls := 0
	s := FromFunc(func() (int, bool, error) {
		calls++
		if calls >= 3 {
			return 0, false, boom
		}
		return calls, true, nil
	})
	var got []int
	endCalled := false
	ec := s.pipeline.evaluate(&recordSink[int]{
		accept: func(v int) bool { got = append(got, v); return true },
		onEnd:  func() { endCalled = true },
	})
	if len(got) != 2 {
		t.Errorf("部分结果 = %v, 期望前 2 个", got)
	}
	if ec.err != boom {
		t.Errorf("ec.err = %v, 期望 boom", ec.err)
	}
	if s.pipeline.err != boom {
		t.Errorf("错误未写回流实例: %v", s.pipeline.err)
	}
	if !endCalled {
		t.Error("出错路径 End 应被调用")
	}
}

func TestSliceTrySplit(t *testing.T) {
	sp := newSliceSp([]int{1, 2, 3, 4, 5, 6, 7}, Sized|Ordered)
	front := sp // TrySplit 后自身收缩为前半段
	back := sp.TrySplit()
	if back == nil {
		t.Fatal("7 元素应可分裂")
	}
	frontItems := collectSplitterator(front)
	backItems := collectSplitterator(back)
	all := append(append([]int(nil), frontItems...), backItems...)
	if len(all) != 7 {
		t.Fatalf("分裂后总数 %d, 期望 7（并集完整）", len(all))
	}
	// 有序源下前后段各自有序且整体有序
	for i := 1; i < len(all); i++ {
		if all[i] != all[i-1]+1 {
			t.Fatalf("分裂破坏顺序: %v", all)
		}
	}
	// 不可再分的下限：剩 1 个元素时返回 nil
	sp1 := newSliceSp([]int{9}, Sized|Ordered)
	if sp1.TrySplit() != nil {
		t.Error("单元素源 TrySplit 应返回 nil")
	}
}

func TestRangeTrySplit(t *testing.T) {
	sp := newRangeSp[int64](0, 10, Sized|Ordered|SubSized)
	back := sp.TrySplit()
	if back == nil {
		t.Fatal("区间应可分裂")
	}
	frontItems := collectSplitterator(sp)
	backItems := collectSplitterator(back)
	if len(frontItems) != 5 || len(backItems) != 5 {
		t.Fatalf("前后段 %d/%d, 期望 5/5", len(frontItems), len(backItems))
	}
	for i, v := range frontItems {
		if v != int64(i) {
			t.Fatalf("前段[%d] = %d, 期望 %d", i, v, i)
		}
	}
	for i, v := range backItems {
		if v != int64(5+i) {
			t.Fatalf("后段[%d] = %d, 期望 %d", i, v, 5+i)
		}
	}
}

func TestConcat(t *testing.T) {
	a := Of(1, 2)
	b := Of(3, 4)
	got := collectViaProbe(Concat(a, b))
	if len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Errorf("Concat 结果 = %v, 期望 [1 2 3 4]", got)
	}
	// 协议：下游仅收到一次 Begin/End
	beginCnt, endCnt := 0, 0
	Concat(Of(1), Of(2)).pipeline.evaluate(&recordSink[int]{
		accept:  func(int) bool { return true },
		onBegin: func(int64) { beginCnt++ },
		onEnd:   func() { endCnt++ },
	})
	if beginCnt != 1 || endCnt != 1 {
		t.Errorf("Begin/End 调用 %d/%d 次, 期望各 1 次", beginCnt, endCnt)
	}
	// nil 容错
	if got := collectViaProbe(Concat[int](nil, Of(1))); len(got) != 1 {
		t.Error("Concat(nil, b) 应返回 b")
	}
}

func TestNilSafety(t *testing.T) {
	// nil 函数/seq 降级为空流而非 panic（编程 bug 才 panic，nil 参数属可预期输入）
	if got := collectViaProbe(Generate[int](nil)); len(got) != 0 {
		t.Error("Generate(nil) 应为空流")
	}
	if got := collectViaProbe(Iterate[int](0, nil)); len(got) != 0 {
		t.Error("Iterate(nil) 应为空流")
	}
	if got := collectViaProbe(FromSeq[int](nil)); len(got) != 0 {
		t.Error("FromSeq(nil) 应为空流")
	}
	if got := collectViaProbe(FromFunc[int](nil)); len(got) != 0 {
		t.Error("FromFunc(nil) 应为空流")
	}
}

// collectViaProbe 以探针 sink 求值并返回收到的元素（测试辅助）。
func collectViaProbe[T any](s *Stream[T]) []T {
	var got []T
	s.pipeline.evaluate(&recordSink[T]{accept: func(v T) bool {
		got = append(got, v)
		return true
	}})
	return got
}

// collectSplitterator 收集源的全部元素（测试辅助）。
func collectSplitterator[T any](sp Splitterator[T]) []T {
	var got []T
	sp.ForEachRemaining(func(v T) bool {
		got = append(got, v)
		return true
	})
	return got
}
