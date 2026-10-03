package stream_test

import (
	"fmt"
	"time"

	"github.com/JayceChant/go-stream"
	"github.com/JayceChant/go-stream/collector"
)

// time_window_example_test.go：时间窗口分桶 TimeWindow 的可运行示例。
// 桶级聚合（重采样）由 TimeWindow(...).Map(...) 组合表达，不设独立聚合入口。

// 时间窗口分桶：按分钟分入对齐时间网格的翻转窗口（桶序=键首现序、桶内保遇序）。
func Example_timeWindow() {
	type tick struct {
		at  time.Time
		vol int
	}
	base := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	ticks := []tick{
		{base.Add(5 * time.Second), 3},
		{base.Add(50 * time.Second), 5},
		{base.Add(20 * time.Second), 7}, // 晚到：并入首桶（桶不拆分）
		{base.Add(65 * time.Second), 2},
	}
	tsOf := func(t tick) time.Time { return t.at }

	// 分桶：TimeWindow 产出 TimeBucket 流（Start + Items）
	for _, b := range stream.TimeWindow(stream.FromSlice(ticks), tsOf, time.Minute).ToSlice() {
		fmt.Printf("%s n=%d\n", b.Start.Format("15:04"), len(b.Items))
	}

	// 桶级聚合（组合表达）：每分钟总量
	sums := stream.TimeWindow(stream.FromSlice(ticks), tsOf, time.Minute).
		Map(func(b stream.TimeBucket[tick]) int {
			total := 0
			for _, t := range b.Items {
				total += t.vol
			}
			return total
		}).
		ToSlice()
	fmt.Println("sums:", sums)
	// Output:
	// 09:00 n=3
	// 09:01 n=1
	// sums: [15 2]
}

// 乱序上游 + 时间序：TimeWindow 桶序=键首现序，SortedByTime 显式按 Start 升序
// （免写比较器的配套形态；升序上游无需本算子）。
func Example_sortedByTime() {
	type tick struct {
		at  time.Time
		vol int
	}
	base := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	ticks := []tick{
		{base.Add(2 * time.Minute), 1}, // 首现序：09:02 桶先出现
		{base, 2},                      // 09:00 桶后出现
		{base.Add(2*time.Minute + 5*time.Second), 3},
	}
	for _, b := range stream.SortedByTime(stream.TimeWindow(stream.FromSlice(ticks), func(t tick) time.Time { return t.at }, time.Minute)).ToSlice() {
		fmt.Printf("%s n=%d\n", b.Start.Format("15:04"), len(b.Items))
	}
	// Output:
	// 09:00 n=1
	// 09:02 n=2
}

// 桶级聚合的收集器形态：桶内元素交任意 collector 处理。
func Example_timeWindowCollect() {
	type tick struct {
		at  time.Time
		vol int
	}
	base := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	ticks := []tick{
		{base.Add(5 * time.Second), 3},
		{base.Add(50 * time.Second), 5},
		{base.Add(65 * time.Second), 2},
	}
	tsOf := func(t tick) time.Time { return t.at }

	// 每分钟计数：TimeWindow + Map，桶内子流交 collector.Counting
	kvs := stream.TimeWindow(stream.FromSlice(ticks), tsOf, time.Minute).
		Map(func(b stream.TimeBucket[tick]) stream.KV[time.Time, int64] {
			return stream.KV[time.Time, int64]{
				Key:   b.Start,
				Value: stream.FromSlice(b.Items).Collect(collector.Counting[tick]()),
			}
		}).
		ToSlice()
	for _, kv := range kvs {
		fmt.Printf("%s n=%d\n", kv.Key.Format("15:04"), kv.Value)
	}
	// Output:
	// 09:00 n=2
	// 09:01 n=1
}
