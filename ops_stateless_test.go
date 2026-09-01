package stream

import (
	"errors"
	"iter"
	"testing"
)

func TestFilterAndPeek(t *testing.T) {
	var peeked []int
	got := collectViaProbe(Of(1, 2, 3, 4, 5, 6).
		Filter(func(v int) bool { return v%2 == 0 }).
		Peek(func(v int) { peeked = append(peeked, v) }))
	if len(got) != 3 || got[0] != 2 || got[2] != 6 {
		t.Errorf("Filter+Peek 结果 = %v, 期望 [2 4 6]", got)
	}
	if len(peeked) != 3 {
		t.Errorf("Peek 副作用 %d 次, 期望 3", len(peeked))
	}
}

func TestMapTypeMigration(t *testing.T) {
	got := collectViaProbe(Of(1, 2, 3).Map(func(v int) string {
		if v == 1 {
			return "一"
		}
		return "多"
	}))
	if len(got) != 3 || got[0] != "一" {
		t.Errorf("Map 结果 = %v", got)
	}
	// 链式类型迁移 int → string → int
	got2 := collectViaProbe(Of(1, 2).Map(func(v int) string { return "x" }).Map(func(s string) int { return len(s) }))
	if len(got2) != 2 || got2[0] != 1 {
		t.Errorf("链式 Map = %v, 期望 [1 1]", got2)
	}
}

func TestFlatMapAndSeq(t *testing.T) {
	got := collectViaProbe(Of(1, 2, 3).FlatMap(func(v int) []int {
		return []int{v, v * 10}
	}))
	want := []int{1, 10, 2, 20, 3, 30}
	if len(got) != len(want) {
		t.Fatalf("FlatMap 结果 = %v, 期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FlatMap[%d] = %d, 期望 %d", i, got[i], want[i])
		}
	}

	// FlatMapSeq：惰性子序列
	seqOf := func(n int) iter.Seq[int] {
		return func(yield func(int) bool) {
			for i := 0; i < n; i++ {
				if !yield(i) {
					return
				}
			}
		}
	}
	got2 := collectViaProbe(Of(1, 2).FlatMapSeq(seqOf))
	if len(got2) != 3 || got2[2] != 1 { // [0, 0 1]
		t.Errorf("FlatMapSeq 结果 = %v, 期望 [0 0 1]", got2)
	}
}

func TestTakeDropWhile(t *testing.T) {
	// TakeWhile 短路：无限源可终止
	got := collectViaProbe(Iterate(1, func(v int) int { return v + 1 }).
		TakeWhile(func(v int) bool { return v <= 5 }))
	if len(got) != 5 || got[4] != 5 {
		t.Errorf("TakeWhile 结果 = %v, 期望 [1..5]", got)
	}
	// DropWhile：丢弃首批后全部放行
	got2 := collectViaProbe(Of(1, 2, 3, 1, 2).
		DropWhile(func(v int) bool { return v < 3 }))
	if len(got2) != 3 || got2[0] != 3 {
		t.Errorf("DropWhile 结果 = %v, 期望 [3 1 2]", got2)
	}
	// 全部满足 TakeWhile 谓词的有限源：正常耗尽
	if got3 := collectViaProbe(Of(1, 2).TakeWhile(func(int) bool { return true })); len(got3) != 2 {
		t.Errorf("全满足 TakeWhile = %v, 期望 2 个", got3)
	}
}

func TestMapErrShortCircuit(t *testing.T) {
	boom := errParseFailed
	calls := 0
	s := Of("1", "2", "x", "4").MapErr(func(s string) (int, error) {
		calls++
		return parseDigit(s)
	})
	var got []int
	endCalled := false
	ec := s.pipeline.evaluate(&recordSink[int]{
		accept: func(v int) bool { got = append(got, v); return true },
		onEnd:  func() { endCalled = true },
	})
	if len(got) != 2 {
		t.Errorf("部分结果 = %v, 期望 [1 2]", got)
	}
	if !errors.Is(ec.firstErr(), boom) {
		t.Errorf("首错 = %v, 期望 boom", ec.firstErr())
	}
	if calls != 3 {
		t.Errorf("转换调用 %d 次, 期望 3（错误短路）", calls)
	}
	if !endCalled {
		t.Error("End 应被调用")
	}
	// Err 写回实例
	if !errors.Is(s.pipeline.err, boom) {
		t.Errorf("实例 err = %v", s.pipeline.err)
	}
}

// errParseFailed 共享错误实例（供 errors.Is/== 断言复用）。
var errParseFailed = errors.New("转换失败")

func parseDigit(s string) (int, error) {
	if len(s) != 1 || s[0] < '0' || s[0] > '9' {
		return 0, errParseFailed
	}
	return int(s[0] - '0'), nil
}

func TestFilterErrPeekErrFlatMapErr(t *testing.T) {
	// FilterErr
	boom := errors.New("谓词失败")
	s := Of(1, 2, 3).FilterErr(func(v int) (bool, error) {
		if v == 2 {
			return false, boom
		}
		return true, nil
	})
	got := collectViaProbe(s)
	if len(got) != 1 || !errors.Is(s.pipeline.err, boom) {
		t.Errorf("FilterErr = %v, err = %v", got, s.pipeline.err)
	}

	// PeekErr
	s2 := Of(1, 2).PeekErr(func(int) error { return boom })
	collectViaProbe(s2)
	if !errors.Is(s2.pipeline.err, boom) {
		t.Errorf("PeekErr err = %v", s2.pipeline.err)
	}

	// FlatMapErr
	s3 := Of(1, 2).FlatMapErr(func(v int) ([]int, error) {
		if v == 1 {
			return nil, boom
		}
		return []int{v}, nil
	})
	got3 := collectViaProbe(s3)
	if len(got3) != 0 || !errors.Is(s3.pipeline.err, boom) {
		t.Errorf("FlatMapErr = %v, err = %v", got3, s3.pipeline.err)
	}
}

func TestNilCallbackPanic(t *testing.T) {
	// nil 回调属编程错误：panic
	cases := []func(){
		func() { Of(1).Filter(nil) },
		func() { Of(1).Map[int](nil) },
		func() { Of(1).FlatMap[int](nil) },
		func() { Of(1).Peek(nil) },
		func() { Of(1).TakeWhile(nil) },
		func() { Of(1).DropWhile(nil) },
		func() { Of(1).MapErr[int](nil) },
	}
	for i, c := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("case %d 应 panic", i)
				}
			}()
			c()
		}()
	}
}

func TestCharsPropagation(t *testing.T) {
	// FromSlice 源特征：SpSized|SpOrdered|SpSubSized

	// Filter 保留全部
	if c := base2().Filter(func(int) bool { return true }).chars; c&SpSized == 0 || c&SpOrdered == 0 {
		t.Errorf("Filter 后特征位 = %b, SpSized/SpOrdered 应保留", c)
	}
	// Map（1:1 变换）保留 SpSized，清除 SpSorted/SpDistinct
	if c := base2().Map(func(v int) int { return v }).chars; c&SpSized == 0 || c&SpSorted != 0 || c&SpDistinct != 0 {
		t.Errorf("Map 后特征位 = %b, SpSized 应保留、SpSorted/SpDistinct 应清除", c)
	}
	// FlatMap（1:N 变换）清除 SpSized
	if c := base2().FlatMap(func(v int) []int { return []int{v} }).chars; c&SpSized != 0 {
		t.Errorf("FlatMap 后特征位 = %b, SpSized 应清除", c)
	}
	// Limit 保持 SpSized 且清 SpSorted
	if c := base2().Limit(2).chars; c&SpSized == 0 || c&SpSorted != 0 {
		t.Errorf("Limit 后特征位 = %b", c)
	}
	// TakeWhile 清除 SpSized
	if c := base2().TakeWhile(func(int) bool { return true }).chars; c&SpSized != 0 {
		t.Errorf("TakeWhile 后特征位 = %b, SpSized 应清除", c)
	}
	// SpSorted 置 SpSorted
	if c := base2().Sorted(func(a, b int) int { return a - b }).chars; c&SpSorted == 0 {
		t.Errorf("SpSorted 后特征位 = %b, SpSorted 应置位", c)
	}
}

// base2 每次生成新的 SpSized 源（避免一次性消费冲突）。
func base2() *Stream[int] {
	return FromSlice([]int{1, 2, 3})
}
