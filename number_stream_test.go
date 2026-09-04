package stream

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/JayceChant/go-stream/collector"
)

// number_stream_test.go：NumberStream 数值流（Task 18）。
// 覆盖：收窄入口（OfNumber/FromNumberSlice/Range/MapToNumber/AsNumber/AsStream）、
// 19 个核心方法、双向桥接一次性语义、提升逃逸与并行/生命周期链。

// expectPanic 断言 fn 必然 panic（编程错误契约）。
func expectPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s 应 panic", name)
		}
	}()
	fn()
}

// ---- 收窄入口 ----

func TestOfNumberAndFromNumberSlice(t *testing.T) {
	if got := OfNumber(3, 1, 2).Sorted().ToSlice(); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("OfNumber+Sorted = %v, 期望 [1 2 3]", got)
	}
	if got := OfNumber[int]().Sum(); got != 0 {
		t.Errorf("OfNumber 空流 Sum = %d, 期望 0", got)
	}
	xs := []int64{5, 4, 9}
	if hi, ok := FromNumberSlice(xs).Max(); !ok || hi != 9 {
		t.Errorf("FromNumberSlice Max = %d, %v", hi, ok)
	}
	// 浮点与无符号类型同样适用
	if got := OfNumber(1.5, 2.25).Sum(); got != 3.75 {
		t.Errorf("OfNumber float Sum = %v", got)
	}
	if got := OfNumber[uint8](200, 55).Sum(); got != 255 {
		t.Errorf("OfNumber uint8 Sum = %d", got)
	}
}

func TestMapToNumber(t *testing.T) {
	words := []string{"go", "stream", "number"}
	sum := FromSlice(words).MapToNumber(func(s string) int { return len(s) }).Sum()
	if sum != 14 { // 2 + 6 + 6
		t.Errorf("MapToNumber Sum = %d, 期望 14", sum)
	}
	avg := FromSlice(words).MapToNumber(func(s string) float64 { return float64(len(s)) }).Avg()
	if avg != 14.0/3 {
		t.Errorf("MapToNumber Avg = %v, 期望 %v", avg, 14.0/3)
	}
	// 1:1 变换保留 SpSized（与 Map 一致，下游可按 size 预分配）
	if c := FromSlice(words).MapToNumber(func(s string) int { return len(s) }).chars; c&SpSized == 0 {
		t.Errorf("MapToNumber 应保留 SpSized, chars = %d", c)
	}
	expectPanic(t, "MapToNumber-nil", func() { Of(1).MapToNumber[int](nil) })
}

func TestAsNumberBridge(t *testing.T) {
	if AsNumber[int](nil) != nil {
		t.Error("AsNumber(nil) 应返回 nil")
	}
	s := Of(1, 2, 3, 4)
	ns := AsNumber(s)
	if got := ns.Filter(func(v int) bool { return v%2 == 0 }).Sum(); got != 6 {
		t.Errorf("AsNumber 链式 Sum = %d, 期望 6", got)
	}
	// 桥接即消费：原流继续使用 panic
	expectPanic(t, "AsNumber-后复用原流", func() { s.Count() })
	// 二次桥接 panic
	s2 := Of(1)
	AsNumber(s2)
	expectPanic(t, "AsNumber-二次桥接", func() { AsNumber(s2) })
}

func TestAsStreamBridge(t *testing.T) {
	var nilNS *NumberStream[int]
	if nilNS.AsStream() != nil {
		t.Error("AsStream(nil) 应返回 nil")
	}
	ns := OfNumber(1, 2, 3)
	st := ns.AsStream()
	if got := st.Map(func(v int) int { return v * 10 }).ToSlice(); !slices.Equal(got, []int{10, 20, 30}) {
		t.Errorf("AsStream 后 Map = %v", got)
	}
	// 本句柄已消费
	expectPanic(t, "AsStream-后复用本流", func() { ns.Sum() })
	// 被遮蔽的比较器形态经 AsStream 使用
	got := OfNumber(3, 1, 2).AsStream().
		Sorted(func(a, b int) int { return b - a }).
		ToSlice()
	if !slices.Equal(got, []int{3, 2, 1}) {
		t.Errorf("AsStream+比较器 Sorted = %v", got)
	}
}

// ---- 元素保持型中间操作 ----

func TestNumberStreamIntermediateOps(t *testing.T) {
	even := func(v int) bool { return v%2 == 0 }
	if got := Range(0, 10).Filter(even).ToSlice(); !slices.Equal(got, []int{0, 2, 4, 6, 8}) {
		t.Errorf("Filter = %v", got)
	}
	calls := 0
	if got := Range(1, 5).Peek(func(int) { calls++ }).Sum(); got != 10 || calls != 4 {
		t.Errorf("Peek+Sum = %d, calls = %d", got, calls)
	}
	if got := OfNumber(1, 2, 3, 1).TakeWhile(func(v int) bool { return v < 3 }).ToSlice(); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("TakeWhile = %v", got)
	}
	if got := OfNumber(1, 2, 3, 1).DropWhile(func(v int) bool { return v < 3 }).ToSlice(); !slices.Equal(got, []int{3, 1}) {
		t.Errorf("DropWhile = %v", got)
	}
	if got := Range(0, 100).Limit(3).ToSlice(); !slices.Equal(got, []int{0, 1, 2}) {
		t.Errorf("Limit = %v", got)
	}
	if got := OfNumber(1).Limit(0).ToSlice(); len(got) != 0 {
		t.Errorf("Limit(0) 应为空流, got %v", got)
	}
	if got := Range(0, 5).Skip(2).ToSlice(); !slices.Equal(got, []int{2, 3, 4}) {
		t.Errorf("Skip = %v", got)
	}
	if got := Range(0, 3).Reverse().ToSlice(); !slices.Equal(got, []int{2, 1, 0}) {
		t.Errorf("Reverse = %v", got)
	}
}

func TestNumberStreamSkipZeroIdentity(t *testing.T) {
	ns := OfNumber(1, 2)
	if got := ns.Skip(0); got != ns {
		t.Error("Skip(0) 应恒等返回自身")
	}
	// 恒等不消费：仍可求值一次
	if got := ns.Sum(); got != 3 {
		t.Errorf("Skip(0) 后 Sum = %d, 期望 3", got)
	}
	// 求值后本句柄作废
	expectPanic(t, "Skip(0)-求值后复用", func() { ns.Count() })
}

// ---- 自然序收窄 ----

func TestNumberStreamNaturalOrderOps(t *testing.T) {
	if got := OfNumber(5, 3, 9, 1).Sorted().ToSlice(); !slices.Equal(got, []int{1, 3, 5, 9}) {
		t.Errorf("Sorted = %v", got)
	}
	if got := OfNumber(5, 3, 9, 1).StableSorted().ToSlice(); !slices.Equal(got, []int{1, 3, 5, 9}) {
		t.Errorf("StableSorted = %v", got)
	}
	if got := OfNumber(3, 1, 3, 2, 1).Distinct().ToSlice(); !slices.Equal(got, []int{3, 1, 2}) {
		t.Errorf("Distinct = %v, 期望保首见遇序 [3 1 2]", got)
	}
	if got := OfNumber(3, 1, 3, 2).Distinct().Sorted().ToSlice(); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("Distinct+Sorted = %v", got)
	}
	// 浮点 NaN 互不相等：各 NaN 均保留（同 map 键语义）
	nans := OfNumber(1.0, math.NaN(), 1.0).Distinct().ToSlice()
	if len(nans) != 2 || nans[0] != 1.0 || !math.IsNaN(nans[1]) {
		t.Errorf("Distinct NaN = %v, 期望 [1 NaN]", nans)
	}
}

// ---- 标志与生命周期 ----

func TestNumberStreamParallelAndLifecycle(t *testing.T) {
	// 并行 Max/Min 与串行一致（分片归并语义不变）
	if v, ok := Range(0, 1000).Parallel(4).Max(); !ok || v != 999 {
		t.Errorf("并行 Max = %d, %v", v, ok)
	}
	if v, ok := Range(0, 1000).Parallel(4).Min(); !ok || v != 0 {
		t.Errorf("并行 Min = %d, %v", v, ok)
	}
	// Sum 为串行求值（与包级 Sum 一致），并行声明下结果不变
	if got := Range(0, 100).Parallel(4).Sum(); got != 4950 {
		t.Errorf("Parallel+Sum = %d, 期望 4950", got)
	}
	// Sequential 抵消后逐元素一致
	want := Range(0, 100).ToSlice()
	if got := Range(0, 100).Parallel(4).Sequential().ToSlice(); !slices.Equal(got, want) {
		t.Errorf("Sequential = %v", got)
	}
	// Unordered 集合一致（顺序不保证）
	got := Range(0, 100).Parallel(4).Unordered().ToSlice()
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("Unordered 集合应与串行一致")
	}
	// OnClose：求值结束自动触发一次，显式 Close 幂等
	var closed atomic.Int32
	ns := Range(0, 10).Parallel(2).
		OnClose(func() error { closed.Add(1); return nil })
	ns.Sum()
	if n := closed.Load(); n != 1 {
		t.Errorf("OnClose 触发 %d 次, 期望 1", n)
	}
	ns.Close()
	if n := closed.Load(); n != 1 {
		t.Errorf("Close 后仍应为 1 次, got %d", n)
	}
}

// ---- 收窄红利终端 ----

func TestNumberStreamTerminals(t *testing.T) {
	if got := OfNumber[int]().Sum(); got != 0 {
		t.Errorf("空流 Sum = %d", got)
	}
	if got := OfNumber[int]().Avg(); got != 0 {
		t.Errorf("空流 Avg = %d", got)
	}
	if v, ok := OfNumber[int]().Min(); ok || v != 0 {
		t.Error("空流 Min 应为 (0, false)")
	}
	if v, ok := OfNumber[int]().Max(); ok || v != 0 {
		t.Error("空流 Max 应为 (0, false)")
	}
	if v, ok := OfNumber(3, 1, 2).Min(); !ok || v != 1 {
		t.Errorf("Min = %d, %v", v, ok)
	}
	if v, ok := OfNumber(3, 1, 2).Max(); !ok || v != 3 {
		t.Errorf("Max = %d, %v", v, ok)
	}
	// Contains 短路：命中即停止遍历
	calls := 0
	if !OfNumber(1, 2, 3).Peek(func(int) { calls++ }).Contains(2) {
		t.Error("Contains 应 true")
	}
	if calls != 2 {
		t.Errorf("Contains 拉取 %d 次, 期望 2（短路）", calls)
	}
	// 整数除法语义（与包级 Avg 一致）
	if got := Range(1, 4).Avg(); got != 2 {
		t.Errorf("Avg = %d, 期望 2（整除）", got)
	}
}

// ---- 提升逃逸（未覆写方法保持 Stream 语义） ----

func TestNumberStreamPromotedEscape(t *testing.T) {
	// 类型迁移自然返回 *Stream：Map 到 string
	if got := Range(1, 4).Map(strconv.Itoa).ToSlice(); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Errorf("Map 逃逸 = %v", got)
	}
	// 值终端直接可用
	if got := Range(0, 10).Count(); got != 10 {
		t.Errorf("Count = %d", got)
	}
	if v, ok := Range(0, 3).First(); !ok || v != 0 {
		t.Errorf("First = %d, %v", v, ok)
	}
	// Collect + collector.Summing 提升
	if got := Range(1, 5).Collect(collector.Summing[int]()); got != 10 {
		t.Errorf("Collect Summing = %d, 期望 10", got)
	}
	// DistinctBy（按键去重）提升
	if got := OfNumber(-1, 1, -2).DistinctBy(func(v int) int { return v * v }).ToSlice(); !slices.Equal(got, []int{-1, -2}) {
		t.Errorf("DistinctBy = %v, 期望 [-1 -2]", got)
	}
	// Err 变体提升：MapErr 后错误槽可查
	es := Range(0, 3).MapErr(func(v int) (int, error) {
		if v == 2 {
			return 0, errors.New("boom")
		}
		return v, nil
	})
	if got := es.ToSlice(); !slices.Equal(got, []int{0, 1}) || es.Err() == nil {
		t.Errorf("MapErr 部分结果 = %v, Err = %v", got, es.Err())
	}
	// FilterErr（元素保持 Err 变体）提升可用
	if got := OfNumber(1, 2).FilterErr(func(v int) (bool, error) { return v > 1, nil }).Count(); got != 1 {
		t.Errorf("FilterErr Count = %d, 期望 1", got)
	}
}

// ---- 一次性语义 ----

func TestNumberStreamOneShot(t *testing.T) {
	ns := OfNumber(1, 2, 3)
	ns.Filter(func(int) bool { return true }) // 链接即消费上游
	expectPanic(t, "链接后复用本流", func() { ns.Sum() })
}
