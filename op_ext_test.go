package stream

import (
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestZipBasic(t *testing.T) {
	// 等长
	got := collectViaProbe(Of(1, 2, 3).Zip(Of("a", "b", "c"), func(n int, s string) string {
		return strconv.Itoa(n) + s
	}))
	if len(got) != 3 || got[0] != "1a" || got[2] != "3c" {
		t.Errorf("Zip 等长 = %v, 期望 [1a 2b 3c]", got)
	}
}

func TestZipTakeShort(t *testing.T) {
	// 取短：b 先耗尽
	got := collectViaProbe(Of(1, 2, 3, 4).Zip(Of(10, 20), func(a, b int) int { return a + b }))
	if len(got) != 2 || got[1] != 22 {
		t.Errorf("Zip 取短(b) = %v, 期望 [11 22]", got)
	}
	// 取短：a 先耗尽
	got2 := collectViaProbe(Of(1).Zip(Of(10, 20), func(a, b int) int { return a * b }))
	if len(got2) != 1 || got2[0] != 10 {
		t.Errorf("Zip 取短(a) = %v, 期望 [10]", got2)
	}
}

func TestZipTerminatesUpstream(t *testing.T) {
	// a 先耗尽时，b 的无限源应被停止（无 goroutine 泄漏、不阻塞）
	var gen atomic.Int32
	got := collectViaProbe(Of(1, 2).Zip(
		Generate(func() int { return int(gen.Add(1)) }),
		func(a, b int) int { return a * b }))
	if len(got) != 2 || got[1] != 4 {
		t.Errorf("Zip 无限源 = %v, 期望 [1 4]", got)
	}
	if g := gen.Load(); g > 3 { // 允许至多多拉一个（缓冲），不应失控
		t.Errorf("无限源被拉动 %d 次, 应及时停止", g)
	}
}

func TestZipErrPropagation(t *testing.T) {
	// 双流错误合并进同一 evalCtx
	boom := errStr("b 侧失败")
	a := Of(1, 2, 3)
	b := FromFunc(func() (int, bool, error) { return 0, false, boom })
	s := a.Zip(b, func(x, y int) int { return x })
	got := collectViaProbe(s)
	if len(got) != 0 {
		t.Errorf("Zip 出错时部分结果 = %v, 期望空", got)
	}
	if !errors.Is(s.pipeline.err, boom) {
		t.Errorf("Zip err = %v, 期望 boom", s.pipeline.err)
	}
}

func TestZipPanicPropagation(t *testing.T) {
	// b 侧用户回调 panic 应原样传播（经 evalCtx.panicVal 中转）
	defer func() {
		if r := recover(); r == nil {
			t.Error("b 侧 panic 应传播")
		}
	}()
	b := Of(1).Peek(func(int) { panic("b 侧回调 panic") })
	Of(10).Zip(b, func(x, y int) int { return x }).pipeline.evaluate(
		&recordSink[int]{accept: func(int) bool { return true }})
}

type errStr string

func (e errStr) Error() string { return string(e) }

func TestWindowSliding(t *testing.T) {
	// 基本滑动：每元素一窗
	got := WindowSliding(Of(1, 2, 3, 4), 2).ToSlice()
	want := [][]int{{1, 2}, {2, 3}, {3, 4}}
	if len(got) != len(want) {
		t.Fatalf("WindowSliding 输出 %d 窗, 期望 %d", len(got), len(want))
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Errorf("窗 %d = %v, 期望 %v", i, got[i], want[i])
		}
	}

	// n == len：单窗即全部元素
	got1 := WindowSliding(Of(1, 2, 3), 3).ToSlice()
	if len(got1) != 1 || !slices.Equal(got1[0], []int{1, 2, 3}) {
		t.Errorf("n==len = %v", got1)
	}

	// 元素少于 n：无输出
	if got0 := WindowSliding(Of(1), 2).ToSlice(); len(got0) != 0 {
		t.Errorf("不足 n 应无输出, got %v", got0)
	}

	// 空流
	if gotE := WindowSliding(Empty[int](), 2).ToSlice(); len(gotE) != 0 {
		t.Errorf("空流应无输出, got %v", gotE)
	}

	// 无限源 + Limit：滑动正常终止
	gotInf := WindowSliding(Iterate(1, func(v int) int { return v + 1 }), 3).Limit(2).ToSlice()
	if len(gotInf) != 2 || !slices.Equal(gotInf[0], []int{1, 2, 3}) || !slices.Equal(gotInf[1], []int{2, 3, 4}) {
		t.Errorf("无限源滑动 = %v", gotInf)
	}

	// 环形回绕正确性：n=4 长序列抽查
	gotW := WindowSliding(Of(1, 2, 3, 4, 5, 6), 4).ToSlice()
	if len(gotW) != 3 || !slices.Equal(gotW[2], []int{3, 4, 5, 6}) {
		t.Errorf("回绕窗 = %v", gotW)
	}

	// 边界：n <= 0 panic；nil 流返回 nil
	expectPanic(t, "WindowSliding 非正", func() { WindowSliding(Of(1), 0) })
	if WindowSliding[int](nil, 2) != nil {
		t.Error("nil 流应返回 nil")
	}

	// 特征位与并行降级断言（同 Chunk 规则）
	s := WindowSliding(Of(1, 2, 3), 2)
	if s.chars&SpSized != 0 {
		t.Error("WindowSliding 应清 SpSized")
	}
	if s.splitN != nil {
		t.Error("WindowSliding 应并行降级（splitN=nil）")
	}
}
