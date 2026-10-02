// Package main 演示时间窗口分桶 TimeWindow（Task 26）：
// 以 ts(v).Truncate(d) 把元素分入对齐时间网格的翻转窗口——固定时间间隔
// 而非固定元素个数（对标 Julia/Scala 时间窗口）；桶级聚合（重采样）由
// TimeWindow(...).Map(...) 组合表达，不设独立聚合入口。
//
// 运行：go -C example run ./timewindow（example 为独立模块，不影响库的测试与覆盖率）
package main

import (
	"fmt"
	"time"

	"github.com/JayceChant/go-stream"
	"github.com/JayceChant/go-stream/collector"
)

// sample 是传感器读数：at 为时间戳、vol 为读数。
type sample struct {
	at  time.Time
	vol float64
}

func main() {
	// 1 秒粒度的原始读数，含晚到数据（乱序回跳）
	base := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	samples := []sample{
		{base.Add(1 * time.Second), 1.5},
		{base.Add(4 * time.Second), 2.0},
		{base.Add(2 * time.Second), 0.5}, // 晚到：并入首个 5 秒桶（桶不拆分）
		{base.Add(6 * time.Second), 3.0},
		{base.Add(11 * time.Second), 4.5},
	}
	tsOf := func(s sample) time.Time { return s.at }

	// ---------- 1. 分桶：TimeWindow 产出 TimeBucket 流（Start + Items） ----------
	// 桶序=键首现序、桶内保遇序、不产空桶（9s 处的空洞没有桶）
	fmt.Println("== TimeWindow 分桶 ==")
	for _, b := range stream.TimeWindow(stream.FromSlice(samples), tsOf, 5*time.Second).ToSlice() {
		fmt.Printf("%s 桶内 %d 条\n", b.Start.Format("15:04:05"), len(b.Items))
	}

	// ---------- 2. 桶级聚合（组合表达） ----------
	fmt.Println("\n== 桶级聚合（TimeWindow + Map）==")

	// 每桶总量：Map 内联聚合（重采样的标准输出形态：1 秒粒度 → 5 秒汇总）
	vols := stream.TimeWindow(stream.FromSlice(samples), tsOf, 5*time.Second).
		Map(func(b stream.TimeBucket[sample]) stream.KV[time.Time, float64] {
			total := 0.0
			for _, s := range b.Items {
				total += s.vol
			}
			return stream.KV[time.Time, float64]{Key: b.Start, Value: total}
		}).
		ToSlice()
	for _, kv := range vols {
		fmt.Printf("%s 累计 %.1f\n", kv.Key.Format("15:04:05"), kv.Value)
	}

	// 每桶计数：桶内子流交任意 collector（此处 Counting）
	kvs := stream.TimeWindow(stream.FromSlice(samples), tsOf, 5*time.Second).
		Map(func(b stream.TimeBucket[sample]) stream.KV[time.Time, int64] {
			return stream.KV[time.Time, int64]{
				Key:   b.Start,
				Value: stream.FromSlice(b.Items).Collect(collector.Counting[sample]()),
			}
		}).
		ToSlice()
	fmt.Print("每桶计数: ")
	for _, kv := range kvs {
		fmt.Printf("[%s %d] ", kv.Key.Format("15:04:05"), kv.Value)
	}
	fmt.Println()

	// 每桶均值：数值流形态（Map 内以 FromNumberSlice 收窄后取 Avg）
	avgs := stream.TimeWindow(stream.FromSlice(samples), tsOf, 5*time.Second).
		Map(func(b stream.TimeBucket[sample]) stream.KV[time.Time, float64] {
			vols := make([]float64, len(b.Items))
			for i, s := range b.Items {
				vols[i] = s.vol
			}
			return stream.KV[time.Time, float64]{
				Key:   b.Start,
				Value: stream.FromNumberSlice(vols).Avg(),
			}
		}).
		ToSlice()
	fmt.Print("每桶均值: ")
	for _, kv := range avgs {
		fmt.Printf("[%s %.2f] ", kv.Key.Format("15:04:05"), kv.Value)
	}
	fmt.Println()
}
