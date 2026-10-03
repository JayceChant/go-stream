package stream

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// time_window_test.go：时间窗口分桶 TimeWindow（Task 26）。
// 覆盖：分桶语义（首现序/保遇序/晚到并入/不产空桶）、Truncate 网格对齐、
// Map 组合的桶级聚合、错误即值路径、panic 矩阵、特征位与并行降级。

// twReading 是时间窗口测试的传感器读数模型：at 为时间戳、vol 为读数。
type twReading struct {
	at  time.Time
	vol int
}

func twTS(r twReading) time.Time { return r.at }

func TestTimeWindowBuckets(t *testing.T) {
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{
		{base.Add(10 * time.Second), 1},
		{base.Add(70 * time.Second), 2},
		{base.Add(20 * time.Second), 3}, // 晚到：并入首桶（桶不拆分）
		{base.Add(130 * time.Second), 4},
		{base.Add(75 * time.Second), 5}, // 晚到：并入第二桶
	}
	got := TimeWindow(FromSlice(rs), twTS, time.Minute).ToSlice()
	// 桶序 = 键首现序（10:00, 10:01, 10:02）；桶内保遇序
	wantStarts := []time.Time{base, base.Add(time.Minute), base.Add(2 * time.Minute)}
	wantVols := [][]int{{1, 3}, {2, 5}, {4}}
	if len(got) != len(wantStarts) {
		t.Fatalf("TimeWindow 桶数 = %d, 期望 %d（%v）", len(got), len(wantStarts), got)
	}
	for i, b := range got {
		if !b.Start.Equal(wantStarts[i]) {
			t.Errorf("桶 %d 起点 %v, 期望 %v", i, b.Start, wantStarts[i])
		}
		gotVols := make([]int, len(b.Items))
		for j, r := range b.Items {
			gotVols[j] = r.vol
		}
		if !slices.Equal(gotVols, wantVols[i]) {
			t.Errorf("桶 %d 元素 %v, 期望 %v", i, gotVols, wantVols[i])
		}
	}

	// 空流：无桶
	if gotE := TimeWindow(Empty[twReading](), twTS, time.Minute).ToSlice(); len(gotE) != 0 {
		t.Errorf("空流应无桶, got %v", gotE)
	}

	// 单桶：全部同窗
	same := []twReading{{base.Add(time.Second), 1}, {base.Add(2 * time.Second), 2}}
	if got1 := TimeWindow(FromSlice(same), twTS, time.Minute).ToSlice(); len(got1) != 1 || len(got1[0].Items) != 2 {
		t.Errorf("同窗元素应并入一桶, got %v", got1)
	}
}

func TestTimeWindowTruncateAlign(t *testing.T) {
	// Truncate 网格对齐：对齐到自零时刻起的 d 整数倍，而非首元素时刻
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{
		{base.Add(30*time.Second + 100*time.Millisecond), 1}, // → 10:00:30 桶
		{base.Add(45 * time.Second), 2},                      // → 10:00:30 桶（同 30s 网格）
		{base.Add(91 * time.Second), 3},                      // → 10:01:30 桶（1 分 31 秒向下截断）
	}
	got := TimeWindow(FromSlice(rs), twTS, 30*time.Second).ToSlice()
	if len(got) != 2 {
		t.Fatalf("30s 网格应 2 桶, got %v", got)
	}
	if !got[0].Start.Equal(base.Add(30*time.Second)) || !got[1].Start.Equal(base.Add(90*time.Second)) {
		t.Errorf("桶起点应对齐 30s 网格: %v / %v", got[0].Start, got[1].Start)
	}
}

func TestTimeWindowMapAgg(t *testing.T) {
	// 桶级聚合（重采样）由 Map 组合表达——不设独立聚合入口的回归演示
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{
		{base.Add(10 * time.Second), 3},
		{base.Add(50 * time.Second), 5},
		{base.Add(20 * time.Second), 7}, // 晚到并入首桶：首桶和 = 15、均值 = 5
		{base.Add(65 * time.Second), 2}, // 第二桶
	}

	// 每桶计数
	counts := TimeWindow(FromSlice(rs), twTS, time.Minute).
		Map(func(b TimeBucket[twReading]) int64 { return int64(len(b.Items)) }).
		ToSlice()
	if len(counts) != 2 || counts[0] != 3 || counts[1] != 1 {
		t.Errorf("每桶计数 = %v, 期望 [3 1]", counts)
	}

	// 每桶求和
	sums := TimeWindow(FromSlice(rs), twTS, time.Minute).
		Map(func(b TimeBucket[twReading]) int {
			total := 0
			for _, r := range b.Items {
				total += r.vol
			}
			return total
		}).
		ToSlice()
	if len(sums) != 2 || sums[0] != 15 || sums[1] != 2 {
		t.Errorf("每桶求和 = %v, 期望 [15 2]", sums)
	}

	// 每桶均值
	avgs := TimeWindow(FromSlice(rs), twTS, time.Minute).
		Map(func(b TimeBucket[twReading]) float64 {
			total := 0
			for _, r := range b.Items {
				total += r.vol
			}
			return float64(total) / float64(len(b.Items))
		}).
		ToSlice()
	if len(avgs) != 2 || avgs[0] != 5 || avgs[1] != 2 {
		t.Errorf("每桶均值 = %v, 期望 [5 2]", avgs)
	}
}

func TestTimeWindowErrAndPanic(t *testing.T) {
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	boom := errStr("时间源失败")

	// 上游出错：不产出任何桶（变换不执行），Err() 可查
	n := 0
	s := TimeWindow(FromFunc(func() (twReading, bool, error) {
		n++
		if n >= 3 {
			return twReading{}, false, boom
		}
		return twReading{at: base.Add(time.Duration(n) * time.Minute), vol: n}, true, nil
	}), twTS, time.Minute)
	if got := s.ToSlice(); len(got) != 0 {
		t.Errorf("上游出错应无桶产出, got %v", got)
	}
	if !errors.Is(s.Err(), boom) {
		t.Errorf("Err() = %v, 期望 boom", s.Err())
	}

	// panic 矩阵：nil ts、非正宽度；nil 流返回 nil
	expectPanic(t, "TimeWindow nil ts", func() { TimeWindow(Of(twReading{}), nil, time.Minute) })
	expectPanic(t, "TimeWindow 零宽度", func() { TimeWindow(Of(twReading{}), twTS, 0) })
	expectPanic(t, "TimeWindow 负宽度", func() { TimeWindow(Of(twReading{}), twTS, -time.Minute) })
	if TimeWindow[twReading](nil, twTS, time.Minute) != nil {
		t.Error("TimeWindow nil 流应返回 nil")
	}
}

func TestTimeWindowCharsAndDegrade(t *testing.T) {
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{{base, 1}, {base.Add(time.Second), 2}}

	// 特征位：物化后置 SpSized/SpSubSized/SpLimited，清 SpSorted/SpDistinct；splitN 零值（并行降级）
	s := TimeWindow(FromSlice(rs), twTS, time.Minute)
	if s.chars&SpSized == 0 || s.chars&SpSubSized == 0 {
		t.Error("TimeWindow 应置 SpSized/SpSubSized（桶数物化后已知）")
	}
	if s.chars&SpLimited == 0 {
		t.Error("TimeWindow 应置 SpLimited（物化输出已知有限，对齐物化型统一规则）")
	}
	if s.chars&SpSorted != 0 || s.chars&SpDistinct != 0 {
		t.Error("TimeWindow 应清 SpSorted/SpDistinct")
	}
	if s.splitN != nil {
		t.Error("TimeWindow 应并行降级（splitN=nil）")
	}

	// 并行声明下降级求值：结果正确
	got := TimeWindow(FromSlice(rs).Parallel(4), twTS, time.Minute).
		Map(func(b TimeBucket[twReading]) int { return len(b.Items) }).
		ToSlice()
	if len(got) != 1 || got[0] != 2 {
		t.Errorf("并行声明下降级求值 = %v, 期望单桶 2 元素", got)
	}

	// 短路终端：回放首个桶后即停（下游取消终止后续桶回放）
	multi := []twReading{
		{base, 1}, {base.Add(time.Second), 2},
		{base.Add(time.Minute), 3},
	}
	first, ok := TimeWindow(FromSlice(multi), twTS, time.Minute).First()
	if !ok || first.Start != base || len(first.Items) != 2 {
		t.Errorf("TimeWindow+First = (%v, %v), 期望首桶 2 元素", first, ok)
	}
}

func TestTimeWindowLimitInfinite(t *testing.T) {
	// 物化型不支持无限源；Limit 先行截断后可用（spec 明示的组合用法）
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	i := 0
	got := TimeWindow(
		Generate(func() twReading {
			i++
			return twReading{at: base.Add(time.Duration(i) * time.Second), vol: i}
		}).Limit(5),
		twTS, 2*time.Second,
	).ToSlice()
	// 时间戳 1..5s，2s 网格：1s→0s 桶（1 个）、2s/3s→2s 桶（2 个）、4s/5s→4s 桶（2 个）
	if len(got) != 3 || len(got[0].Items) != 1 || len(got[1].Items) != 2 || len(got[2].Items) != 2 {
		t.Errorf("无限源+Limit 分桶 = %v, 期望 [1 2 2]", got)
	}
}

func TestTimeWindowMixedLocations(t *testing.T) {
	// 桶键规范化（UTC）：同一瞬间的不同 Location 表示恒落同一桶
	// （time.Time 作 map 键按结构体 ==（含 Location 指针）判等，曾把
	// 混合时区数据的等值瞬间拆成两桶）；Start 恒为 UTC 网格点。
	base := time.Date(2026, 9, 11, 1, 30, 0, 0, time.UTC)
	cst := time.FixedZone("CST", 8*3600)
	nepal := time.FixedZone("+0545", 5*3600+45*60)
	rs := []twReading{
		{base, 1},                // UTC 表示
		{base.In(cst), 2},        // 同一瞬间 +08:00 表示
		{base.In(nepal), 3},      // 同一瞬间 +05:45 表示
		{base.Add(time.Hour), 4}, // 下一小时（不同瞬间）
	}
	got := TimeWindow(FromSlice(rs), twTS, time.Hour).ToSlice()
	if len(got) != 2 {
		t.Fatalf("混合 Location 的等值瞬间应并入同桶：桶数 = %d, 期望 2（%v）", len(got), got)
	}
	if got[0].Start.Location() != time.UTC || !got[0].Start.Equal(base.Truncate(time.Hour)) {
		t.Errorf("首桶 Start = %v, 期望 UTC 网格点 %v", got[0].Start, base.Truncate(time.Hour))
	}
	if len(got[0].Items) != 3 {
		t.Errorf("首桶应含 3 个等值瞬间元素, got %d", len(got[0].Items))
	}
}

func TestSortedByTime(t *testing.T) {
	// 配套排序算子：上游时间乱序时 TimeWindow 输出为键首现序，
	// SortedByTime 按 Start 升序免写比较器。
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{
		{base.Add(2 * time.Minute), 1}, // 首现序：10:02 桶先出现
		{base, 2},                      // 10:00 桶后出现
		{base.Add(2*time.Minute + 5*time.Second), 3},
	}
	got := SortedByTime(TimeWindow(FromSlice(rs), twTS, time.Minute)).ToSlice()
	if len(got) != 2 {
		t.Fatalf("桶数 = %d, 期望 2（%v）", len(got), got)
	}
	if !got[0].Start.Equal(base) || !got[1].Start.Equal(base.Add(2*time.Minute)) {
		t.Errorf("SortedByTime 桶序 = [%v, %v], 期望按 Start 升序", got[0].Start, got[1].Start)
	}
	if got[1].Start.Equal(base.Add(2*time.Minute)) && len(got[1].Items) != 2 {
		t.Errorf("桶内元素 = %d, 期望 2（排序不改桶内）", len(got[1].Items))
	}

	// 特征位：物化排序置 SpSorted/SpLimited，清 SpDistinct
	s := SortedByTime(TimeWindow(FromSlice(rs), twTS, time.Minute))
	if s.chars&SpSorted == 0 || s.chars&SpLimited == 0 {
		t.Error("SortedByTime 应置 SpSorted/SpLimited")
	}
	if s.chars&SpDistinct != 0 {
		t.Error("SortedByTime 应清 SpDistinct")
	}
	if SortedByTime[twReading](nil) != nil {
		t.Error("SortedByTime nil 流应返回 nil")
	}
}

func TestCompleteTimeBuckets(t *testing.T) {
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	mk := func(off time.Duration, vols ...int) TimeBucket[twReading] {
		b := TimeBucket[twReading]{Start: base.Add(off)}
		for _, v := range vols {
			b.Items = append(b.Items, twReading{at: base.Add(off), vol: v})
		}
		return b
	}

	// 基本空档：[t0, t0+3d] → 插 t0+d、t0+2d 两个空桶（Items 为 nil）
	got := CompleteTimeBuckets(Of(mk(0, 1), mk(3*time.Minute, 2)), time.Minute).ToSlice()
	if len(got) != 4 {
		t.Fatalf("补全后桶数 = %d, 期望 4（%v）", len(got), got)
	}
	for i, want := range []time.Duration{0, time.Minute, 2 * time.Minute, 3 * time.Minute} {
		if !got[i].Start.Equal(base.Add(want)) {
			t.Errorf("桶 %d Start = %v, 期望 %v（升序）", i, got[i].Start, base.Add(want))
		}
	}
	if got[1].Items != nil || got[2].Items != nil {
		t.Errorf("空桶 Items 应为 nil, got %v/%v", got[1].Items, got[2].Items)
	}
	if len(got[0].Items) != 1 || len(got[3].Items) != 1 {
		t.Errorf("原桶 Items 不应改变, got %d/%d", len(got[0].Items), len(got[3].Items))
	}

	// 连续无补 + 单桶原样 + 空流
	if got := CompleteTimeBuckets(Of(mk(0, 1), mk(time.Minute, 2)), time.Minute).ToSlice(); len(got) != 2 {
		t.Errorf("连续桶不应补全, got %v", got)
	}
	if got := CompleteTimeBuckets(Of(mk(0, 1)), time.Minute).ToSlice(); len(got) != 1 {
		t.Errorf("单桶不应补全, got %v", got)
	}
	if got := CompleteTimeBuckets(Empty[TimeBucket[twReading]](), time.Minute).ToSlice(); len(got) != 0 {
		t.Errorf("空流不应补全, got %v", got)
	}

	// 非整除空档：桶距 2.5d → 补 2 个空桶后接实桶（间隔不足 d 不再补）
	got2 := CompleteTimeBuckets(Of(mk(0), mk(5*time.Second)), 2*time.Second).ToSlice()
	if len(got2) != 4 { // [0s, 2s空, 4s空, 5s]
		t.Fatalf("非整除空档补全 = %d 桶, 期望 4（%v）", len(got2), got2)
	}

	// TimeWindow 组合端到端：乱序上游 → 排序 → 补全
	rs := []twReading{{base.Add(3 * time.Minute), 1}, {base, 2}}
	endToEnd := CompleteTimeBuckets(SortedByTime(TimeWindow(FromSlice(rs), twTS, time.Minute)), time.Minute).ToSlice()
	if len(endToEnd) != 4 || endToEnd[1].Items != nil {
		t.Errorf("TimeWindow+SortedByTime+CompleteTimeBuckets = %v, 期望 4 桶含空桶", endToEnd)
	}

	// panic 矩阵与 nil 流
	expectPanic(t, "CompleteTimeBuckets 零宽度", func() { CompleteTimeBuckets(Of(mk(0)), 0) })
	expectPanic(t, "CompleteTimeBuckets 负宽度", func() { CompleteTimeBuckets(Of(mk(0)), -time.Minute) })
	if CompleteTimeBuckets[twReading](nil, time.Minute) != nil {
		t.Error("CompleteTimeBuckets nil 流应返回 nil")
	}

	// 特征位：物化型置 SpSized/SpSubSized/SpLimited；splitN 零值（并行降级）
	s := CompleteTimeBuckets(Of(mk(0), mk(time.Minute)), time.Minute)
	if s.chars&SpSized == 0 || s.chars&SpSubSized == 0 || s.chars&SpLimited == 0 {
		t.Error("CompleteTimeBuckets 应置 SpSized/SpSubSized/SpLimited")
	}
	if s.splitN != nil {
		t.Error("CompleteTimeBuckets 应并行降级（splitN=nil）")
	}
}

func TestCompleteTimeBucketsGuard(t *testing.T) {
	// 溢出护栏：宽度与跨度失配（两桶相距超 maxTimeBuckets*d）→ panic 而非天量分配
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	b1 := TimeBucket[twReading]{Start: base}
	b2 := TimeBucket[twReading]{Start: base.Add(time.Duration(maxTimeBuckets+2) * time.Millisecond)}
	expectPanic(t, "CompleteTimeBuckets 补全超上限", func() {
		CompleteTimeBuckets(Of(b1, b2), time.Millisecond).ToSlice()
	})
}

func TestTimeWindowJoinRight(t *testing.T) {
	// 合并集成回归：TimeWindow 输出物化后已知有限（SpLimited），作
	// Join/LeftJoin 右流不被有限性守卫误拦（特征位曾随 feat/join 并行
	// 开发缺失，FromFunc 上游 + Join 右流曾 panic）。
	base := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	rs := []twReading{
		{base, 1},
		{base.Add(30 * time.Second), 2}, // 同分钟桶（2 元素）
		{base.Add(time.Minute), 3},      // 次分钟桶（1 元素）
	}
	right := func() *Stream[TimeBucket[twReading]] { return TimeWindow(FromSlice(rs), twTS, time.Minute) }
	on := func(n int, b TimeBucket[twReading]) bool { return len(b.Items) == n }
	combine := func(n int, b TimeBucket[twReading]) int { return n * len(b.Items) }

	if got := Of(1, 2, 3).Join(right(), on, combine).ToSlice(); !slices.Equal(got, []int{1, 4}) {
		t.Errorf("TimeWindow 作 Join 右流 = %v, 期望 [1 4]（n=1 命中次桶、n=2 命中首桶、n=3 无命中不产出）", got)
	}
	// n=3 未命中：LeftJoin 以 TimeBucket 零值保底恰好一条（0 元素）
	if got := Of(1, 2, 3).LeftJoin(right(), on, combine).ToSlice(); !slices.Equal(got, []int{1, 4, 0}) {
		t.Errorf("TimeWindow 作 LeftJoin 右流 = %v, 期望 [1 4 0]", got)
	}
}
