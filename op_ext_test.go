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

func TestJoinInnerBasic(t *testing.T) {
	// 多命中笛卡尔段：左 [2,3,4] × 右 [6,4]，on 为 t 整除 u
	on := func(t, u int) bool { return u%t == 0 }
	got := Of(2, 3, 4).Join(Of(6, 4), on, func(t, u int) int { return t*10 + u }).ToSlice()
	// 左序（外层）：2→(6,4)、3→(6)、4→(4)；右序（内层）：6 在 4 前
	want := []int{26, 24, 36, 44}
	if !slices.Equal(got, want) {
		t.Errorf("Join = %v, 期望 %v", got, want)
	}
}

func TestJoinInnerNoMatch(t *testing.T) {
	got := Of(1, 2, 3).Join(Empty[int](), func(t, u int) bool { return true },
		func(t, u int) int { return t }).ToSlice()
	if len(got) != 0 {
		t.Errorf("右流为空时 InnerJoin 应为空, got %v", got)
	}
	got2 := Empty[int]().Join(Of(1, 2), func(t, u int) bool { return true },
		func(t, u int) int { return t }).ToSlice()
	if len(got2) != 0 {
		t.Errorf("左流为空时 Join 应为空, got %v", got2)
	}
}

func TestJoinLeftUnmatchedZero(t *testing.T) {
	// 未命中左元素以 U 零值恰产出一条；命中元素产出全部命中对
	type pair struct {
		l int
		r string
	}
	got := Of(1, 2, 3).LeftJoin(
		Of("even:2", "even:4"),
		func(t int, u string) bool {
			n, _ := strconv.Atoi(u[len(u)-1:])
			return n%t == 0 // 1 全命中；2 命中 2/4；3 无命中
		},
		func(t int, u string) pair { return pair{t, u} },
	).ToSlice()
	want := []pair{
		{1, "even:2"}, {1, "even:4"},
		{2, "even:2"}, {2, "even:4"},
		{3, ""}, // 未命中：右元素零值 ""
	}
	if !slices.EqualFunc(got, want, func(a, b pair) bool { return a == b }) {
		t.Errorf("LeftJoin = %v, 期望 %v", got, want)
	}

	// 右流为空：每个左元素恰一条零值产出
	got2 := Of(7, 8).LeftJoin(Empty[string](), func(int, string) bool { return true },
		func(t int, u string) pair { return pair{t, u} }).ToSlice()
	if len(got2) != 2 || got2[0].r != "" || got2[1].r != "" {
		t.Errorf("右流为空时 LeftJoin 应逐元素零值保底, got %v", got2)
	}
}

func TestJoinRightJoinViaLeftJoin(t *testing.T) {
	// RightJoin 语义 = 以右流作接收者调 LeftJoin：每个右元素至少出现一次，
	// 无命中的右元素以左侧零值保底（[0 4]）；命中对与 InnerJoin 一致
	on := func(t, u int) bool { return t == u }
	inner := Of(1, 2, 3).Join(Of(2, 4), on, func(t, u int) [2]int { return [2]int{t, u} }).ToSlice()
	rightOuter := Of(2, 4).LeftJoin(Of(1, 2, 3), func(u, t int) bool { return t == u }, // 参数顺序对调
		func(u, t int) [2]int { return [2]int{t, u} }).ToSlice()
	if !slices.Equal(inner, [][2]int{{2, 2}}) {
		t.Errorf("Join = %v, 期望 [[2 2]]", inner)
	}
	if !slices.Equal(rightOuter, [][2]int{{2, 2}, {0, 4}}) {
		t.Errorf("RightJoin(以 LeftJoin 表达) = %v, 期望 [[2 2] [0 4]]", rightOuter)
	}
}

func TestJoinStreamsConsumedOnce(t *testing.T) {
	// 双流一次性：Join 产物复用 / 输入流复用均 panic
	left, right := Of(1, 2), Of(2)
	s := left.Join(right, func(t, u int) bool { return t == u }, func(t, u int) int { return t })
	s.ToSlice()
	expectPanic(t, "Join 产物复用", func() { s.ToSlice() })
	expectPanic(t, "Join 左流复用", func() { left.Count() })
	expectPanic(t, "Join 右流复用", func() { right.Count() })
}

func TestJoinInfiniteLeftShortCircuit(t *testing.T) {
	// 左流无限 + 输出短路：正常终止且只拉取必要的左元素。
	// 组合函数须容错零值右侧（LeftJoin 未命中元素以 0 传入）
	var gen atomic.Int32
	got := Generate(func() int { return int(gen.Add(1)) }).
		LeftJoin(Of(2), func(t, u int) bool { return t%u == 0 },
			func(t, u int) int {
				if u == 0 {
					return -t // 未命中：右侧零值的标记输出
				}
				return t / u
			}).
		Limit(3).ToSlice()
	if !slices.Equal(got, []int{-1, 1, -3}) { // 1 未命中→-1、2 命中→1、3 未命中→-3
		t.Errorf("无限左流 LeftJoin+Limit = %v, 期望 [-1 1 -3]", got)
	}
	if g := gen.Load(); g > 4 { // 短路后源应及时停止
		t.Errorf("左源被拉动 %d 次, 应及时停止", g)
	}
}

func TestJoinErrPropagation(t *testing.T) {
	boom := errStr("join 失败")
	// right 物化出错：不驱动 left、产出为空、Err() 可查
	right := Of(1, 2, 3).MapErr(func(int) (int, error) { return 0, boom }) // Of 源已声明有限（SpLimited 守卫）
	var leftDriven atomic.Int32
	left := Of(1, 2, 3).Peek(func(int) { leftDriven.Add(1) })
	s := left.Join(right, func(t, u int) bool { return true }, func(t, u int) int { return t })
	got := s.ToSlice()
	if len(got) != 0 {
		t.Errorf("right 出错时 Join 产出 = %v, 期望空", got)
	}
	if !errors.Is(s.pipeline.err, boom) {
		t.Errorf("Join err = %v, 期望 boom", s.pipeline.err)
	}
	if leftDriven.Load() != 0 {
		t.Errorf("right 出错时 left 不应被驱动, 实际拉动 %d 次", leftDriven.Load())
	}

	// left 驱动中出错：保留已产出部分结果、Err() 返回首错
	leftErr := Of(1, 2, 3).MapErr(func(t int) (int, error) {
		if t == 3 {
			return 0, boom
		}
		return t, nil
	})
	s2 := leftErr.Join(Of(1), func(t, u int) bool { return true }, func(t, u int) int { return t*10 + u })
	got2 := s2.ToSlice()
	if !slices.Equal(got2, []int{11, 21}) {
		t.Errorf("left 出错时部分结果 = %v, 期望 [11 21]", got2)
	}
	if !errors.Is(s2.pipeline.err, boom) {
		t.Errorf("left 出错时 err = %v, 期望 boom", s2.pipeline.err)
	}
}

func TestJoinPanicPropagation(t *testing.T) {
	// on/combiner 回调 panic 原样传播（全程发起 goroutine，无中转）
	defer func() {
		if r := recover(); r == nil {
			t.Error("on 回调 panic 应传播")
		}
	}()
	Of(1).Join(Of(1), func(t, u int) bool { panic("on panic") },
		func(t, u int) int { return t }).ToSlice()
}

func TestJoinLeftPanicPropagation(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("combiner 回调 panic 应传播")
		}
	}()
	Of(1).LeftJoin(Of(2), func(t, u int) bool { return true },
		func(t, u int) int { panic("combiner panic") }).ToSlice()
}

func TestJoinCharsAndDowngrade(t *testing.T) {
	// 特征位：双侧按位与后清 Sized/SubSized/Sorted/Distinct；splitN 降级
	s := FromSlice([]int{1, 2, 3}).Join(FromSlice([]int{4, 5}),
		func(t, u int) bool { return true }, func(t, u int) int { return t })
	if s.chars&SpSized != 0 || s.chars&SpSubSized != 0 {
		t.Error("Join 应清 SpSized/SpSubSized")
	}
	if s.chars&SpSorted != 0 || s.chars&SpDistinct != 0 {
		t.Error("Join 应清 SpSorted/SpDistinct")
	}
	if s.chars&SpOrdered == 0 {
		t.Error("双侧有序时 Join 应保 SpOrdered")
	}
	if s.splitN != nil {
		t.Error("Join 应并行降级（splitN=nil）")
	}
	// 有序性经 Unordered 一侧清除
	s2 := FromSlice([]int{1}).Join(FromMap(map[int]int{1: 1}).Map(func(kv KV[int, int]) int { return kv.Key }),
		func(t, u int) bool { return true }, func(t, u int) int { return t })
	if s2.chars&SpOrdered != 0 {
		t.Error("右侧 Unordered 时 Join 应失 SpOrdered")
	}
}

func TestJoinNilArgs(t *testing.T) {
	// 方法形态：other/on/combiner nil panic（对齐 Zip；Join 与 LeftJoin 双方法并存）
	expectPanic(t, "Join other nil", func() {
		Of(1).Join(nil, func(t, u int) bool { return true }, func(t, u int) int { return t })
	})
	expectPanic(t, "Join on nil", func() {
		Of(1).Join(Of(1), nil, func(t, u int) int { return t })
	})
	expectPanic(t, "Join combiner nil", func() {
		Of(1).Join[int, int](Of(1), func(t, u int) bool { return true }, nil)
	})
	expectPanic(t, "LeftJoin other nil", func() {
		Of(1).LeftJoin(nil, func(t, u int) bool { return true }, func(t, u int) int { return t })
	})
	expectPanic(t, "LeftJoin on nil", func() {
		Of(1).LeftJoin(Of(1), nil, func(t, u int) int { return t })
	})
	expectPanic(t, "LeftJoin combiner nil", func() {
		Of(1).LeftJoin[int, int](Of(1), func(t, u int) bool { return true }, nil)
	})
}

func TestJoinOutputShortCircuit(t *testing.T) {
	// 输出短路：downstream 取消后停止驱动左流（右流已物化属预期）
	var leftSeen atomic.Int32
	got := Of(1, 2, 3, 4).Peek(func(int) { leftSeen.Add(1) }).
		Join(FromSlice([]int{1, 1, 1}), func(t, u int) bool { return true },
			func(t, u int) int { return t }).
		Limit(2).ToSlice()
	if !slices.Equal(got, []int{1, 1}) {
		t.Errorf("短路输出 = %v, 期望 [1 1]", got)
	}
	if leftSeen.Load() != 1 {
		t.Errorf("下游取消后左流应停止（拉动 %d 次, 期望 1）", leftSeen.Load())
	}
}

func TestSpLimitedCharacteristics(t *testing.T) {
	// 已知有限：slice/range 族（Sized ⇒ Limited）与 FromMap（有限但不报大小）
	for name, s := range map[string]*Stream[int]{
		"Of":          Of(1, 2),
		"FromSlice":   FromSlice([]int{1}),
		"Empty":       Empty[int](),
		"Range":       Range(0, 3).AsStream(),
		"RangeClosed": RangeClosed(0, 3).AsStream(),
	} {
		if s.chars&SpLimited == 0 {
			t.Errorf("%s 源应置 SpLimited", name)
		}
	}
	if s := FromMap(map[int]int{1: 1}); s.chars&SpLimited == 0 || s.chars&SpSized != 0 {
		t.Errorf("FromMap 应置 SpLimited 且不置 SpSized, got %b", s.chars)
	}
	// 大小未知 / 设计无限：不置位
	for name, s := range map[string]*Stream[int]{
		"FromFunc":    FromFunc(func() (int, bool, error) { return 0, false, nil }),
		"FromSeq":     FromSeq(func(yield func(int) bool) { yield(1) }),
		"FromChannel": FromChannel(make(chan int)),
		"Generate":    Generate(func() int { return 1 }),
		"Iterate":     Iterate(1, func(v int) int { return v + 1 }),
	} {
		if s.chars&SpLimited != 0 {
			t.Errorf("%s 源不应置 SpLimited", name)
		}
	}
}

func TestSpLimitedPropagation(t *testing.T) {
	seq1 := func() *Stream[int] { return FromSeq(func(yield func(int) bool) { yield(1) }) }
	// 透传类：有限进有限出（清 Sized 的算子仍保 Limited）；未知源不虚标
	if c := Of(1, 2).FlatMap(func(v int) []int { return []int{v} }).chars; c&SpLimited == 0 || c&SpSized != 0 {
		t.Errorf("FlatMap 后应保 SpLimited 清 SpSized, got %b", c)
	}
	if c := Of(1, 2).TakeWhile(func(int) bool { return true }).chars; c&SpLimited == 0 {
		t.Errorf("TakeWhile 后应保 SpLimited, got %b", c)
	}
	if c := seq1().Filter(func(int) bool { return true }).chars; c&SpLimited != 0 {
		t.Errorf("未知源 Filter 后不应虚标 SpLimited, got %b", c)
	}
	// 物化类：强制置位（含无限/未知上游）
	if c := Generate(func() int { return 1 }).Limit(3).chars; c&SpLimited == 0 {
		t.Errorf("Generate+Limit 后应置 SpLimited, got %b", c)
	}
	if c := seq1().Sorted(func(a, b int) int { return a - b }).chars; c&SpLimited == 0 {
		t.Errorf("Sorted 后应置 SpLimited, got %b", c)
	}
	// 双流：双侧 AND
	if c := Concat(Of(1), Of(2)).chars; c&SpLimited == 0 {
		t.Errorf("Concat 双侧有限应置 SpLimited, got %b", c)
	}
	if c := Concat(Of(1), Generate(func() int { return 1 })).chars; c&SpLimited != 0 {
		t.Errorf("Concat 一侧无限不应置 SpLimited, got %b", c)
	}
	if c := Of(1).Zip(Of(2), func(a, b int) int { return a }).chars; c&SpLimited == 0 {
		t.Errorf("Zip 双侧有限应置 SpLimited, got %b", c)
	}
	if c := Of(1).Zip(Generate(func() int { return 1 }), func(a, b int) int { return a }).chars; c&SpLimited != 0 {
		t.Errorf("Zip 一侧无限不应置 SpLimited, got %b", c)
	}
	// Join 产物：右流经守卫必有限，左流有限则产物置位
	if c := Of(1, 2).Join(Of(1), func(t, u int) bool { return true }, func(t, u int) int { return t }).chars; c&SpLimited == 0 {
		t.Errorf("Join 双侧有限产物应置 SpLimited, got %b", c)
	}
	if c := seq1().Join(Of(1), func(t, u int) bool { return true }, func(t, u int) int { return t }).chars; c&SpLimited != 0 {
		t.Errorf("Join 左侧大小未知时产物不应置 SpLimited, got %b", c)
	}
}

func TestJoinFiniteGuard(t *testing.T) {
	// 未声明有限（无限或大小未知）的右流：链接期 panic（求值前 fail-fast）
	bad := map[string]func() *Stream[int]{
		"Generate":    func() *Stream[int] { return Generate(func() int { return 1 }) },
		"Iterate":     func() *Stream[int] { return Iterate(1, func(v int) int { return v + 1 }) },
		"FromFunc":    func() *Stream[int] { return FromFunc(func() (int, bool, error) { return 0, false, nil }) },
		"FromSeq":     func() *Stream[int] { return FromSeq(func(yield func(int) bool) { yield(1) }) },
		"FromChannel": func() *Stream[int] { return FromChannel(make(chan int)) },
		"TakeWhile 无限": func() *Stream[int] {
			return Iterate(1, func(v int) int { return v + 1 }).TakeWhile(func(int) bool { return true })
		},
		"Concat 含无限": func() *Stream[int] { return Concat(Of(1), Generate(func() int { return 1 })) },
	}
	for name, mk := range bad {
		expectPanic(t, "Join 右流 "+name, func() {
			Of(1).Join(mk(), func(t, u int) bool { return true }, func(t, u int) int { return t })
		})
		expectPanic(t, "LeftJoin 右流 "+name, func() {
			Of(1).LeftJoin(mk(), func(t, u int) bool { return true }, func(t, u int) int { return t })
		})
	}
	// 逃生路径：Limit 上界（物化置位）；无限源换作左流
	if got := Of(2, 3).
		Join(Generate(func() int { return 1 }).Limit(1), func(t, u int) bool { return true },
			func(t, u int) int { return t*10 + u }).ToSlice(); !slices.Equal(got, []int{21, 31}) {
		t.Errorf("Generate+Limit 作右流 = %v, 期望 [21 31]", got)
	}
	if got := Generate(func() int { return 1 }).
		Join(Of(1), func(t, u int) bool { return true },
			func(t, u int) int { return t }).Limit(2).ToSlice(); len(got) != 2 {
		t.Errorf("无限源作左流应正常, got %v", got)
	}
}
