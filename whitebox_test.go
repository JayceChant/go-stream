package stream

import (
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/JayceChant/go-stream/collector"
)

// whitebox_test.go：Task 11 白盒补缺——按覆盖审计清单直测引擎内部路径。
//
// 覆盖对象：splitSrc/splitNOf、runParts 并行 panic、newStateful 错误路径、
// Concat 段包装器与错误路径、运行期并行降级、pushPart 取消、源级释放语义、
// collectingSink 容量边界、evalCtx 并发语义、特征位传播矩阵与 splitN 降级点。

// ---- splitSrc / splitNOf ----

// drainSp 拉空一个子源返回元素（测试辅助）。
func drainSp[T any](sp Splitterator[T]) []T {
	var out []T
	sp.ForEachRemaining(func(v T) bool { out = append(out, v); return true })
	return out
}

func TestSplitSrcBasic(t *testing.T) {
	// n<2：无需再分，整体一份（元素不丢失）。
	src := newSliceSp([]int{1, 2, 3, 4, 5, 6, 7}, SpSized|SpOrdered)
	for _, n := range []int{0, 1} {
		parts := splitSrc(src, n)
		if len(parts) != 1 {
			t.Fatalf("splitSrc(n=%d) = %d 份, 期望 1（整体一份）", n, len(parts))
		}
	}
	// n<2 不消耗源（TrySplit 未被调用）。
	if src.i != 0 || len(src.s) != 7 {
		t.Errorf("n<2 分片消耗了源（i=%d len=%d）", src.i, len(src.s))
	}
}

func TestSplitSrcTwoParts(t *testing.T) {
	src := newSliceSp([]int{1, 2, 3, 4}, SpSized|SpOrdered)
	parts := splitSrc(src, 2)
	if len(parts) != 2 {
		t.Fatalf("splitSrc(n=2) 份数 = %d, 期望 2", len(parts))
	}
	front := drainSp(parts[0].(Splitterator[int]))
	back := drainSp(parts[1].(Splitterator[int]))
	if !slices.Equal(front, []int{1, 2}) || !slices.Equal(back, []int{3, 4}) {
		t.Errorf("二分 = [%v | %v], 期望 [[1 2] | [3 4]]", front, back)
	}
}

func TestSplitSrcOddN(t *testing.T) {
	// 奇数 n 不对称递归：前半 n/2=2 份、后半 n-n/2=1 份（10 元素 → 3 段）。
	src := newSliceSp([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, SpSized|SpOrdered)
	parts := splitSrc(src, 3)
	if len(parts) != 3 {
		t.Fatalf("splitSrc(n=3) 份数 = %d, 期望 3", len(parts))
	}
	var merged []int
	for _, p := range parts {
		merged = append(merged, drainSp(p.(Splitterator[int]))...)
	}
	if !slices.Equal(merged, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Errorf("并集 = %v, 期望 0..9 保序", merged)
	}
}

func TestSplitSrcDeepRecur(t *testing.T) {
	// 深递归：可分能力不足时返回份数 < n（各子源至少 1 元素）。
	src := newSliceSp([]int{1, 2, 3}, SpSized|SpOrdered)
	parts := splitSrc(src, 8)
	if len(parts) > 3 {
		t.Errorf("3 元素源 n=8 得 %d 份, 应 ≤3", len(parts))
	}
	var merged []int
	for _, p := range parts {
		merged = append(merged, drainSp(p.(Splitterator[int]))...)
	}
	if !slices.Equal(merged, []int{1, 2, 3}) {
		t.Errorf("并集 = %v, 期望 [1 2 3]", merged)
	}
}

func TestSplitSrcUnsplittableSinglePart(t *testing.T) {
	// 单元素源：TrySplit 返回 nil → 整体一份（非 nil，元素不丢失）。
	src := newSliceSp([]int{1}, SpSized|SpOrdered)
	parts := splitSrc(src, 4)
	if len(parts) != 1 {
		t.Fatalf("单元素源 splitSrc = %d 份, 期望 1", len(parts))
	}
	if got := drainSp(parts[0].(Splitterator[int])); !slices.Equal(got, []int{1}) {
		t.Errorf("单份并集 = %v, 期望 [1]", got)
	}
}

func TestParallelSmallInputNoLoss(t *testing.T) {
	// 回归（Task 11 修复）：小输入 + 深并行分片不丢元素——
	// 修复前 splitSrc 递归深处不可再分的子源被整段丢弃
	//（如 [1,2,3].Parallel(4) 丢 [1]、[1,2].Parallel(4) 只剩 [1]）。
	for _, size := range []int{1, 2, 3, 4, 5, 6, 7} {
		for _, n := range []int{2, 3, 4, 8} {
			data := make([]int, size)
			for i := range data {
				data[i] = i
			}
			got := FromSlice(data).Parallel(n).ToSlice()
			if !slices.Equal(got, data) {
				t.Errorf("len=%d Parallel(%d) = %v, 期望 %v（元素丢失）", size, n, got, data)
			}
			if c := FromSlice(slices.Clone(data)).Parallel(n).Count(); c != int64(size) {
				t.Errorf("len=%d Parallel(%d).Count() = %d, 期望 %d", size, n, c, size)
			}
		}
	}
}

func TestSplitNOfLazy(t *testing.T) {
	// splitNOf 构造不消耗源；调用闭包才分片。
	src := newSliceSp([]int{1, 2, 3, 4}, SpSized|SpOrdered)
	fn := splitNOf(src)
	if src.i != 0 || len(src.s) != 4 {
		t.Fatalf("splitNOf 构造即消耗源（i=%d len=%d）", src.i, len(src.s))
	}
	parts := fn(2)
	if len(parts) != 2 {
		t.Fatalf("splitNOf(2) = %d 份, 期望 2", len(parts))
	}
	// 分片后 src 收缩为前半段。
	if len(src.s) != 2 || src.i != 0 {
		t.Errorf("分片后源未收缩为前半段（len=%d i=%d）", len(src.s), src.i)
	}
}

// ---- runParts：并行片内 panic 捕获与 re-panic ----

func TestParallelPanicRePanic(t *testing.T) {
	// 有序并行路径：片内用户回调 panic 被捕获，收尾时按捕获序 re-panic。
	s := FromSlice([]int{0, 1, 2, 3, 4, 5, 6, 7}).Parallel(2).
		Peek(func(v int) {
			if v == 7 {
				panic("boom-part")
			}
		})
	defer func() {
		if r := recover(); r != nil {
			if msg, ok := r.(string); !ok || msg != "boom-part" {
				t.Errorf("re-panic 值 = %v, 期望 boom-part", r)
			}
		} else {
			t.Error("片内 panic 应 re-panic")
		}
	}()
	s.ToSlice()
	t.Error("不可达：应已 panic")
}

func TestUnorderedParallelPanicRePanic(t *testing.T) {
	// 无序流式合并路径：panic 暂存，排空 done 后 re-panic。
	s := FromSlice([]int{0, 1, 2, 3, 4, 5, 6, 7}).Parallel(2).Unordered().
		Peek(func(v int) {
			if v == 0 {
				panic("boom-unordered")
			}
		})
	defer func() {
		if r := recover(); r != nil {
			if msg, ok := r.(string); !ok || msg != "boom-unordered" {
				t.Errorf("re-panic 值 = %v, 期望 boom-unordered", r)
			}
		} else {
			t.Error("无序路径片内 panic 应 re-panic")
		}
	}()
	s.ToSlice()
	t.Error("不可达：应已 panic")
}

// ---- newStateful 错误路径 ----

func TestStatefulErrSkipsProcess(t *testing.T) {
	// 上游出错：process 不执行，Begin(0)/End 仍配对下发。
	processCalled := false
	s := FromFunc(func() (int, bool, error) { return 0, false, errors.New("upstream") })
	got := s.Limit(3) // 走 newStateful；借 Reverse 语义位观察 process
	_ = got
	s2 := FromFunc(func() (int, bool, error) { return 0, false, errors.New("upstream") })
	ns := newStateful(s2, -1, func(buf []int) []int {
		processCalled = true
		return buf
	}, SpOrdered)
	var beginSize int64 = -99
	var endCnt int
	ns.pipeline.evaluate(&recordSink[int]{
		accept:  func(int) bool { return true },
		onBegin: func(sz int64) { beginSize = sz },
		onEnd:   func() { endCnt++ },
	})
	if processCalled {
		t.Error("上游出错时 process 不应执行")
	}
	if beginSize != 0 {
		t.Errorf("错误路径 Begin(0) 收到 %d", beginSize)
	}
	if endCnt != 1 {
		t.Errorf("End 调用 %d 次, 期望 1", endCnt)
	}
	if err := ns.Err(); err == nil || err.Error() != "upstream" {
		t.Errorf("Err() = %v, 期望 upstream", err)
	}
}

// ---- Concat 错误路径与段包装器 ----

func TestConcatErrSkipsB(t *testing.T) {
	// a 段出错：b 段不驱动，End 恰一次，Err 为 a 侧首错。
	aErr := errors.New("a-fail")
	a := FromFunc(func() (int, bool, error) { return 0, false, aErr })
	bDriven := 0
	b := Of(9, 10).Peek(func(int) { bDriven++ })
	var endCnt int
	var got []int
	s := Concat(a, b)
	s.pipeline.evaluate(&recordSink[int]{
		accept: func(v int) bool { got = append(got, v); return true },
		onEnd:  func() { endCnt++ },
	})
	if bDriven != 0 {
		t.Errorf("a 段出错后 b 仍被驱动 %d 次", bDriven)
	}
	if len(got) != 0 {
		t.Errorf("a 段出错时结果 = %v, 期望空", got)
	}
	if endCnt != 1 {
		t.Errorf("End 调用 %d 次, 期望 1", endCnt)
	}
	if s.Err() != aErr {
		t.Errorf("Err() = %v, 期望 %v", s.Err(), aErr)
	}
}

func TestConcatWrapperSinks(t *testing.T) {
	// suppressEnd：透传 Begin/Accept、吞 End；skipBegin：吞 Begin、透传 End。
	var log []string
	inner := &recordSink[int]{
		accept:  func(int) bool { log = append(log, "accept"); return true },
		onBegin: func(int64) { log = append(log, "begin") },
		onEnd:   func() { log = append(log, "end") },
	}
	suppressEnd[int]{inner}.Begin(1)
	suppressEnd[int]{inner}.Accept(1)
	suppressEnd[int]{inner}.End()
	if !slices.Equal(log, []string{"begin", "accept"}) {
		t.Errorf("suppressEnd 日志 = %v, 期望 [begin accept]（End 被吞）", log)
	}
	log = nil
	skipBegin[int]{inner}.Begin(1)
	skipBegin[int]{inner}.Accept(2)
	skipBegin[int]{inner}.End()
	if !slices.Equal(log, []string{"accept", "end"}) {
		t.Errorf("skipBegin 日志 = %v, 期望 [accept end]（Begin 被吞）", log)
	}
}

// ---- 运行期并行降级与 nil splitN 包装 ----

func TestParallelRuntimeFallback(t *testing.T) {
	// splitN 非 nil 但求值期返回 nil（单元素源）：len(parts)<2 降级串行。
	var beginSize int64 = -99
	s := FromSlice([]int{42}).Parallel(4) // splitN 设置但 TrySplit 失败
	s.pipeline.evaluateNP(&recordSink[int]{
		accept:  func(int) bool { return true },
		onBegin: func(sz int64) { beginSize = sz },
	}, sliceTotal[int]{})
	if !s.pipeline.consumed {
		t.Error("求值后 consumed 应置位")
	}
	_ = beginSize // 降级串行路径正常完成即验证（值不作强断言）
}

func TestParallelAfterDegradedOpNilSplitN(t *testing.T) {
	// 降级算子（Sorted 置 splitN=nil）之后再 Parallel：包装返回 nil，求值串行。
	s := FromSlice([]int{3, 1, 2}).Sorted(func(a, b int) int { return a - b }).Parallel(4)
	if s.splitN == nil {
		t.Fatal("Parallel stage 的 splitN 包装不应为 nil 本身")
	}
	if got := s.splitN(4); got != nil {
		t.Errorf("降级算子后 splitN(4) = %d 份, 期望 nil", len(got))
	}
	got := s.ToSlice()
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("降级后并行求值结果 = %v, 期望 [1 2 3]", got)
	}
}

// ---- streamTotal.pushPart 取消路径（白盒直测）----

// cancelTotal 是白盒测试用 streamTotal：推送首个元素后即请求取消。
type cancelTotal[T any] struct {
	fired bool
}

func (c *cancelTotal[T]) part() Sink[T] {
	return &collectingSink[T]{limit: -1}
}

func (c *cancelTotal[T]) total(parts []Sink[T], down Sink[T], _ *evalCtx) {
	for _, ps := range parts {
		for _, v := range ps.(*collectingSink[T]).buf {
			if !down.Accept(v) {
				return
			}
		}
	}
}

func (c *cancelTotal[T]) pushPart(i int, sinks []Sink[T], down Sink[T], _ *evalCtx) bool {
	for _, v := range sinks[i].(*collectingSink[T]).buf {
		if !down.Accept(v) {
			c.fired = true
			return false // 取消：验证引擎停止后续推送
		}
	}
	return true
}

func TestStreamMergeCancelStopsPush(t *testing.T) {
	// 白盒直测 evaluateParallelStream 的 cancelled 分支：pushPart 返回 false
	// 后，后续分片不再推送（终端只收到首个元素）。
	var got []int
	ct := &cancelTotal[int]{}
	s := FromSlice([]int{0, 1, 2, 3, 4, 5, 6, 7}).Parallel(2).Unordered()
	// 终端 Accept 首个元素后返回 false（取消）。
	s.pipeline.evaluateNP(&recordSink[int]{
		accept: func(v int) bool {
			got = append(got, v)
			return false
		},
	}, ct)
	if !ct.fired {
		t.Error("pushPart 取消路径未触发")
	}
	if len(got) != 1 {
		t.Errorf("取消后终端收到 %d 个元素, 期望 1", len(got))
	}
}

// ---- sliceTotal.total 回放短路 ----

func TestSliceTotalReplayShortCircuit(t *testing.T) {
	// 物化回放中终端取消：停止本片及后续片（break 分支）。
	parts := []Sink[int]{
		&collectingSink[int]{buf: []int{1, 2}},
		&collectingSink[int]{buf: []int{3, 4}},
	}
	var got []int
	sliceTotal[int]{}.total(parts, &recordSink[int]{
		accept: func(v int) bool {
			got = append(got, v)
			return len(got) < 1 // 首个元素后取消
		},
	}, &evalCtx{})
	if !slices.Equal(got, []int{1}) {
		t.Errorf("回放短路收到 %v, 期望 [1]", got)
	}
}

// ---- 源级路径 ----

func TestSeqSpCancelReleases(t *testing.T) {
	// seqSp 取消（f 返回 false）：iter.Pull 的 stop 被调用（defer 标志）。
	released := false
	seq := func(yield func(int) bool) {
		defer func() { released = true }()
		for i := 0; i < 100; i++ {
			if !yield(i) {
				return
			}
		}
	}
	sp := newSeqSp(seq)
	advanced := sp.TryAdvance(func(int) bool { return false }) // 请求取消
	if advanced {
		t.Error("取消后 TryAdvance 应返回 false")
	}
	if !released {
		t.Error("取消后 stop 未被调用（迭代器未释放）")
	}
}

func TestChannelSpShortCircuit(t *testing.T) {
	// channelSp 短路：f 返回 false 提前返回，不排空通道。
	ch := make(chan int, 8)
	for i := 1; i <= 8; i++ {
		ch <- i
	}
	sp := newChannelSp(ch)
	if sp.TryAdvance(func(int) bool { return false }) {
		t.Error("短路后 TryAdvance 应返回 false")
	}
	if len(ch) != 7 {
		t.Errorf("短路后通道剩余 %d, 期望 7（未排空）", len(ch))
	}
}

func TestFuncSpErrCached(t *testing.T) {
	// funcSp 首错缓存：出错后再次 TryAdvance 不再调用 next。
	calls := 0
	wantErr := errors.New("sp-err")
	sp := newFuncSp(func() (int, bool, error) {
		calls++
		return 0, false, wantErr
	})
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("出错后 TryAdvance 应返回 false")
	}
	if calls != 1 {
		t.Fatalf("next 调用 %d 次, 期望 1", calls)
	}
	if sp.TryAdvance(func(int) bool { return true }) {
		t.Error("首错缓存后 TryAdvance 应恒 false")
	}
	if calls != 1 {
		t.Errorf("首错缓存后 next 仍被调用（共 %d 次）", calls)
	}
}

// ---- collectingSink 容量边界 ----

func TestCollectingSinkCapacity(t *testing.T) {
	// size>0 且无限额：预分配 size。
	c := &collectingSink[int]{limit: -1}
	c.Begin(4)
	if cap(c.buf) != 4 {
		t.Errorf("Begin(4) 后 cap = %d, 期望 4", cap(c.buf))
	}
	// size>0 且 limit<size：截到 limit。
	c2 := &collectingSink[int]{limit: 2}
	c2.Begin(10)
	if cap(c2.buf) != 2 {
		t.Errorf("Begin(10)/limit=2 后 cap = %d, 期望 2", cap(c2.buf))
	}
	// size<=0：不预分配（buf 保持 nil）。
	c3 := &collectingSink[int]{limit: -1}
	c3.Begin(-1)
	if c3.buf != nil {
		t.Errorf("Begin(-1) 后 buf = %v, 期望 nil", c3.buf)
	}
	// limit=0：首次 Accept 即拒绝。
	c4 := &collectingSink[int]{limit: 0}
	c4.Begin(4)
	if c4.Accept(1) {
		t.Error("limit=0 首次 Accept 应返回 false")
	}
}

// ---- evalCtx 并发语义 ----

func TestEvalCtxConcurrentFail(t *testing.T) {
	// 多 goroutine 并发 fail：最终 err 恰为其中之一（非 nil、无撕裂）。
	ec := &evalCtx{}
	errs := []error{
		errors.New("e1"), errors.New("e2"), errors.New("e3"), errors.New("e4"),
	}
	var wg sync.WaitGroup
	wg.Add(len(errs))
	for _, e := range errs {
		go func(e error) {
			defer wg.Done()
			ec.fail(e)
		}(e)
	}
	wg.Wait()
	got := ec.firstErr()
	if !slices.Contains(errs, got) {
		t.Errorf("并发 fail 后 firstErr = %v, 应为四者之一", got)
	}
}

func TestEvalCtxTakePanicOnce(t *testing.T) {
	// takePanic 一次性读取：第二次调用返回 nil。
	ec := &evalCtx{}
	ec.mu.Lock()
	ec.panicVal = "p"
	ec.mu.Unlock()
	if v := ec.takePanic(); v != "p" {
		t.Errorf("首次 takePanic = %v, 期望 p", v)
	}
	if v := ec.takePanic(); v != nil {
		t.Errorf("二次 takePanic = %v, 期望 nil（一次性语义）", v)
	}
}

// ---- 特征位传播矩阵与 splitN 降级点 ----

// sizedSource 返回 SpSized|SpOrdered|SpSubSized 源流（特征位矩阵基底）。
func sizedSource() *Stream[int] {
	return FromSlice([]int{1, 2, 3})
}

func TestCharsMatrixStateless(t *testing.T) {
	// Peek / FlatMapSeq：全保留 / 清 SpSized+SpSorted+SpDistinct。
	if c := sizedSource().Peek(func(int) {}).chars; c&(SpSized|SpOrdered|SpSubSized) != SpSized|SpOrdered|SpSubSized {
		t.Errorf("Peek 后特征位 = %b, SpSized/SpOrdered/SpSubSized 应全保留", c)
	}
	if c := sizedSource().FlatMapSeq(func(v int) iter.Seq[int] {
		return func(yield func(int) bool) { yield(v) }
	}).chars; c&SpSized != 0 {
		t.Errorf("FlatMapSeq 后特征位 = %b, SpSized 应清除", c)
	}
}

func TestCharsMatrixErrVariants(t *testing.T) {
	// MapErr 保 SpSized；FilterErr 全保留；PeekErr 全保留；FlatMapErr 清 SpSized。
	if c := sizedSource().MapErr(func(v int) (int, error) { return v, nil }).chars; c&SpSized == 0 || c&SpSorted != 0 {
		t.Errorf("MapErr 后特征位 = %b, SpSized 保留、SpSorted 清除", c)
	}
	if c := sizedSource().FilterErr(func(int) (bool, error) { return true, nil }).chars; c&SpSized == 0 {
		t.Errorf("FilterErr 后特征位 = %b, SpSized 应保留", c)
	}
	if c := sizedSource().PeekErr(func(int) error { return nil }).chars; c&SpSized == 0 {
		t.Errorf("PeekErr 后特征位 = %b, SpSized 应保留", c)
	}
	if c := sizedSource().FlatMapErr(func(v int) ([]int, error) { return nil, nil }).chars; c&SpSized != 0 {
		t.Errorf("FlatMapErr 后特征位 = %b, SpSized 应清除", c)
	}
}

func TestCharsMatrixStateful(t *testing.T) {
	// Skip 强制置 SpSized|SpSubSized（含无 sized 上游）；Limit 同；Sorted 置 SpSorted；
	// DistinctBy 置 SpDistinct；Reverse 全透传。
	if c := sizedSource().Skip(1).chars; c&(SpSized|SpSubSized) != SpSized|SpSubSized {
		t.Errorf("Skip 后特征位 = %b, SpSized/SpSubSized 应置位", c)
	}
	// 无 sized 上游（FromSeq 仅 SpOrdered）：Skip 仍强制置 SpSized|SpSubSized。
	if c := FromSeq(seqIntsOf(1)).Skip(1).chars; c&SpSized == 0 || c&SpSubSized == 0 {
		t.Errorf("无 sized 上游 Skip 后特征位 = %b, SpSized/SpSubSized 仍应强制置位", c)
	}
	if c := sizedSource().Limit(2).chars; c&(SpSized|SpSubSized) != SpSized|SpSubSized || c&SpSorted != 0 {
		t.Errorf("Limit 后特征位 = %b, SpSized/SpSubSized 置位、SpSorted 清除", c)
	}
	if c := sizedSource().Sorted(func(a, b int) int { return a - b }).chars; c&SpSorted == 0 || c&SpSized == 0 {
		t.Errorf("Sorted 后特征位 = %b, SpSorted 置位、SpSized 保留", c)
	}
	if c := sizedSource().DistinctBy(func(v int) any { return v }).chars; c&SpDistinct == 0 || c&SpSorted != 0 {
		t.Errorf("DistinctBy 后特征位 = %b, SpDistinct 置位、SpSorted 清除", c)
	}
	if c := sizedSource().Reverse().chars; c != sizedSource().chars {
		t.Errorf("Reverse 后特征位 = %b, 期望全透传 %b", c, sizedSource().chars)
	}
}

func TestCharsMatrixSinglePass(t *testing.T) {
	// 单遍有状态算子：清 SpSized/SpSorted/SpDistinct，且 splitN=nil（并行降级）。
	clear3 := func(c Characteristics) bool {
		return c&SpSized == 0 && c&SpSorted == 0 && c&SpDistinct == 0
	}
	if c := sizedSource().DropWhile(func(int) bool { return true }).chars; !clear3(c) {
		t.Errorf("DropWhile 后特征位 = %b, 三位应清除", c)
	}
	if s := sizedSource().DropWhile(func(int) bool { return true }); s.splitN != nil {
		t.Error("DropWhile 后 splitN 应为 nil（并行降级）")
	}
	if c := sizedSource().Scan(0, func(a, v int) int { return a + v }).chars; !clear3(c) {
		t.Errorf("Scan 后特征位 = %b, 三位应清除", c)
	}
	if s := sizedSource().Scan(0, func(a, v int) int { return a + v }); s.splitN != nil {
		t.Error("Scan 后 splitN 应为 nil（并行降级）")
	}
	if c := Chunk(sizedSource(), 2).chars; !clear3(c) {
		t.Errorf("Chunk 后特征位 = %b, 三位应清除", c)
	}
	if s := Chunk(sizedSource(), 2); s.splitN != nil {
		t.Error("Chunk 后 splitN 应为 nil（并行降级）")
	}
	if c := Enumerate(sizedSource()).chars; !clear3(c) {
		t.Errorf("Enumerate 后特征位 = %b, 三位应清除", c)
	}
	if s := Enumerate(sizedSource()); s.splitN != nil {
		t.Error("Enumerate 后 splitN 应为 nil（并行降级）")
	}
}

func TestCharsMatrixComposite(t *testing.T) {
	// Concat：并集 &^ SpSized；Zip：交集 &^ (SpSized|SpSorted|SpDistinct)；Parallel 透传。
	if c := Concat(sizedSource(), sizedSource()).chars; c&SpSized != 0 || c&SpOrdered == 0 {
		t.Errorf("Concat 后特征位 = %b, SpSized 清除、SpOrdered 保留", c)
	}
	if c := sizedSource().Zip(sizedSource(), func(a, b int) int { return a + b }).chars; c&SpSized != 0 || c&SpOrdered == 0 {
		t.Errorf("Zip 后特征位 = %b, SpSized 清除、SpOrdered 保留", c)
	}
	if c := sizedSource().Parallel(4).chars; c != sizedSource().chars {
		t.Errorf("Parallel 后特征位 = %b, 期望透传 %b", c, sizedSource().chars)
	}
}

func TestCharsMatrixSources(t *testing.T) {
	// 生成器型源仅 SpOrdered；Of/Range 含 SpSized|SpSubSized；FromMap 无 SpOrdered。
	if c := FromSeq(seqIntsOf(1)).chars; c != SpOrdered {
		t.Errorf("FromSeq 源特征位 = %b, 期望仅 SpOrdered", c)
	}
	if c := FromChannel(make(chan int)).chars; c != SpOrdered {
		t.Errorf("FromChannel 源特征位 = %b, 期望仅 SpOrdered", c)
	}
	if c := Generate(func() int { return 1 }).chars; c != SpOrdered {
		t.Errorf("Generate 源特征位 = %b, 期望仅 SpOrdered", c)
	}
	if c := Range(0, 3).chars; c&(SpSized|SpSubSized|SpOrdered) != SpSized|SpSubSized|SpOrdered {
		t.Errorf("Range 源特征位 = %b, 期望 SpSized|SpSubSized|SpOrdered", c)
	}
	if c := Of(1).chars; c&(SpSized|SpSubSized) != SpSized|SpSubSized {
		t.Errorf("Of 源特征位 = %b, 期望含 SpSized|SpSubSized", c)
	}
}

// seqInts / seqIntsOf 是特征位矩阵测试用的 iter.Seq 辅助。
type seqInts = func(yield func(int) bool) // 恒等别名（FlatMapSeq 返回类型占位）

func seqIntsOf(n int) func(yield func(int) bool) {
	return func(yield func(int) bool) {
		for i := 0; i < n; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// ---- 有序并行 Min/Max 与 Collect 无 Combiner 降级 ----

func TestParallelOrderedMinMax(t *testing.T) {
	// 有序并行路径的 Min/Max（此前仅无序流式路径被测）。
	data := []int{7, 3, 9, 1, 8, 2, 6, 5, 4, 0}
	cmp := func(a, b int) int { return a - b }
	lo, ok1 := FromSlice(data).Parallel(4).Min(cmp)
	hi, ok2 := FromSlice(data).Parallel(4).Max(cmp)
	if !ok1 || lo != 0 {
		t.Errorf("有序并行 Min = (%d, %v), 期望 (0, true)", lo, ok1)
	}
	if !ok2 || hi != 9 {
		t.Errorf("有序并行 Max = (%d, %v), 期望 (9, true)", hi, ok2)
	}
}

func TestCollectNoCombinerParallelDegrades(t *testing.T) {
	// Combiner 为 nil 的收集器：pt 为 nil，并行声明自动串行，结果仍正确。
	data := []int{1, 2, 3, 4, 5, 6, 7, 8}
	c := collector.Collector[int, *int64, int64]{
		Supplier:    func() *int64 { return new(int64) },
		Accumulator: func(n *int64, v int) { *n += int64(v) },
		// Combiner 为 nil：Collect 构造的 parallelTotal 为 nil → 串行
		Finisher: func(n *int64) int64 { return *n },
	}
	got := FromSlice(data).Parallel(4).Collect(c)
	if got != 36 {
		t.Errorf("无 Combiner 并行 Collect = %d, 期望 36（串行降级）", got)
	}
}
