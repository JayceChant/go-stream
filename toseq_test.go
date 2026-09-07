package stream

import (
	"errors"
	"slices"
	"testing"
)

// toseq_test.go：Task 19——ToSeq 出站适配（流 → iter.Seq）单测。

func TestToSeqRange(t *testing.T) {
	// range-over-func 全量消费
	var got []int
	for v := range Of(1, 2, 3).Map(func(v int) int { return v * 2 }).ToSeq() {
		got = append(got, v)
	}
	if !slices.Equal(got, []int{2, 4, 6}) {
		t.Errorf("ToSeq range = %v, 期望 [2 4 6]", got)
	}
}

func TestToSeqBreakShortCircuit(t *testing.T) {
	// 中途 break：源遍历短路停止
	pulled := 0
	s := FromFunc(func() (int, bool, error) {
		pulled++
		return pulled, true, nil
	})
	var got []int
	for v := range s.ToSeq() {
		got = append(got, v)
		if v == 3 {
			break
		}
	}
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("break 后已产出 = %v, 期望 [1 2 3]", got)
	}
	if pulled != 3 {
		t.Errorf("源被拉动 %d 次, 期望 3（短路生效）", pulled)
	}
}

func TestToSeqSlicesCollect(t *testing.T) {
	// 与标准库互通：slices.Collect 接受 iter.Seq
	got := slices.Collect(Of("a", "b").ToSeq())
	if !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("slices.Collect = %v", got)
	}
}

func TestToSeqErrValue(t *testing.T) {
	// 错误即值：FromFunc 出错，部分产出 + Err() 可查
	wantErr := errors.New("boom")
	s := FromFunc(func() (int, bool, error) {
		return 0, false, wantErr
	})
	var n int
	for range s.ToSeq() {
		n++
	}
	if n != 0 {
		t.Errorf("出错流产出 %d 个元素, 期望 0", n)
	}
	if !errors.Is(s.Err(), wantErr) {
		t.Errorf("Err() = %v, 期望 boom", s.Err())
	}
}

func TestToSeqOnCloseTriggered(t *testing.T) {
	// OnClose 回调链随求值结束照常触发
	closed := false
	s := Of(1).OnClose(func() error { closed = true; return nil })
	for range s.ToSeq() {
	}
	if !closed {
		t.Error("OnClose 回调未随 ToSeq 求值触发")
	}
}

func TestToSeqSecondRangePanics(t *testing.T) {
	// 一次性契约：二次 range 同一 seq 触发重复消费 panic
	seq := Of(1, 2, 3).ToSeq()
	for range seq { // 首遍消费
	}
	defer func() {
		if recover() == nil {
			t.Error("二次 range 应 panic（一次性语义）")
		}
	}()
	for range seq { // 二次 range：流已被消费
	}
}

func TestToSeqNumberStream(t *testing.T) {
	// NumberStream 经提升直接可用
	var got []int
	for v := range Range(1, 4).ToSeq() {
		got = append(got, v)
	}
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("NumberStream ToSeq = %v, 期望 [1 2 3]", got)
	}
}
