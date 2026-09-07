// Package main 演示流扩展第一批（Task 19~23，对齐 Java 25 Stream）：
// ToSeq 出站迭代适配、Collector 组合生态、WindowSliding 滑动窗口、
// Summary 单遍统计、RangeClosed/OfNonZero 便捷源。
//
// 运行：go -C example run ./extensions（example 为独立模块，不影响库的测试与覆盖率）
package main

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/JayceChant/go-stream"
	"github.com/JayceChant/go-stream/collector"
)

// order 是贯穿 Collector 组合生态示例的业务模型。
type order struct {
	user   string
	amount float64
}

var orders = []order{
	{"Alice", 120.5}, {"Bob", 89.0}, {"Alice", 45.0},
	{"Carol", 230.0}, {"Bob", 60.5},
}

func main() {
	// amountOf 把订单映射为金额：下游收集器的元素类型经 Mapping 从 order 迁移为 float64。
	amountOf := func(o order) float64 { return o.amount }

	// ---------- 1. ToSeq：流 → iter.Seq 出站适配（Task 19） ----------
	// 把流编译为 Go 1.23 push 迭代器：range-over-func 直接消费，
	// 或交给任何接受 iter.Seq 的标准库 API（slices.Collect 等）。
	// 中途 break 即短路——源遍历随消费方提前终止而停止。
	fmt.Println("== ToSeq 出站适配 ==")
	var first3 []int
	for v := range stream.Of(1, 2, 3, 4, 5).ToSeq() {
		first3 = append(first3, v)
		if v == 3 {
			break // 短路：4、5 不会被产出
		}
	}
	fmt.Println("break 后已产出:", first3) // [1 2 3]
	fmt.Println("slices.Collect:", slices.Collect(stream.Of("a", "b").ToSeq()))

	// ---------- 2. WindowSliding：滑动窗口（Task 21） ----------
	// 只输出满窗（长度恰 n），元素少于 n 无输出——移动平均的标准姿势。
	fmt.Println("\n== WindowSliding 滑动窗口 ==")
	prices := []float64{10, 12, 9, 14, 15, 13}
	windows := stream.WindowSliding(stream.FromSlice(prices), 3).ToSlice()
	fmt.Println("3 日窗口:", windows)
	avg3 := stream.WindowSliding(stream.FromSlice(prices), 3).
		Map(func(w []float64) float64 {
			sum := 0.0
			for _, p := range w {
				sum += p
			}
			return sum / float64(len(w))
		}).
		ToSlice()
	fmt.Printf("3 日移动平均: %.2f\n", avg3)

	// ---------- 3. Collector 组合生态（Task 20） ----------
	fmt.Println("\n== Collector 组合生态 ==")

	// GroupingByDownstream：两级汇聚——分组后每组交下游收集器（这里数笔数）
	counts := stream.FromSlice(orders).Collect(collector.GroupingByDownstream(
		func(o order) string { return o.user },
		collector.Counting[order](),
	))
	fmt.Println("每人订单数:", counts) // map[Alice:2 Bob:2 Carol:1]

	// GroupingByDownstream + Mapping + Summing：分组求和
	sums := stream.FromSlice(orders).Collect(collector.GroupingByDownstream(
		func(o order) string { return o.user },
		collector.Mapping(amountOf, collector.Summing[float64]()),
	))
	fmt.Println("每人总额:", sums)

	// Teeing：一次遍历同时求和与计数，merge 合并为均值
	avgAmount := stream.FromSlice(orders).Collect(collector.Teeing(
		collector.Mapping(amountOf, collector.Summing[float64]()),
		collector.Counting[order](),
		func(s float64, n int64) float64 { return s / float64(n) },
	))
	fmt.Printf("订单均值: %.2f\n", avgAmount)

	// PartitioningBySlice：布尔分组（两侧各为切片）
	part := stream.FromSlice(orders).Collect(collector.PartitioningBySlice(
		func(o order) bool { return o.amount >= 100 },
	))
	fmt.Println("大额订单数:", len(part.True), "小额订单数:", len(part.False))

	// MinBy/MaxBy：收集器形态最值（空流返回零值）
	minA := stream.FromSlice(orders).Map(amountOf).
		Collect(collector.MinBy(cmp.Compare[float64]))
	maxA := stream.FromSlice(orders).Map(amountOf).
		Collect(collector.MaxBy(cmp.Compare[float64]))
	fmt.Printf("最小/最大金额: %.1f / %.1f\n", minA, maxA)

	// CollectingAndThen：finisher 包装——结果再加工（求和后格式化为文案）
	bill := stream.FromSlice(orders).Collect(collector.CollectingAndThen(
		collector.Mapping(amountOf, collector.Summing[float64]()),
		func(s float64) string { return fmt.Sprintf("营业额合计 %.1f 元", s) },
	))
	fmt.Println(bill)

	// Filtering：作为下游收集器的过滤（组内过滤，不影响其它组的元素流）
	big := stream.FromSlice(orders).Collect(collector.GroupingByDownstream(
		func(o order) string { return o.user },
		collector.Filtering(func(o order) bool { return o.amount >= 80 },
			collector.Counting[order]()),
	))
	fmt.Println("每人 >=80 的订单数:", big)

	// ---------- 4. Summary：单遍统计（Task 22） ----------
	// 一次遍历同时产出 count/sum/min/max，Avg() 派生免二次遍历。
	fmt.Println("\n== Summary 单遍统计 ==")
	stats := stream.Summary(stream.FromSlice(prices))
	fmt.Println(stats)
	fmt.Printf("Count=%d Sum=%.1f Min=%.1f Max=%.1f\n",
		stats.Count, stats.Sum, stats.Min, stats.Max)
	// 收集器形态（可组合，如分组统计）：
	byUser := stream.FromSlice(orders).Collect(collector.GroupingByDownstream(
		func(o order) string { return o.user },
		collector.Mapping(amountOf, collector.Summarizing[float64]()),
	))
	fmt.Println("Alice 统计:", byUser["Alice"])

	// ---------- 5. RangeClosed / OfNonZero：便捷源（Task 23） ----------
	fmt.Println("\n== RangeClosed / OfNonZero ==")
	// 闭区间 [1, 5]（含两端；Range 是左闭右开 [1, 5)）
	fmt.Println("RangeClosed(1,5):", stream.RangeClosed(1, 5).ToSlice())
	fmt.Println("RangeClosed(1,100) 求和:", stream.RangeClosed(1, 100).Sum())
	// OfNonZero 过滤零值元素（zero 涵盖 nil——对齐 cmp.Or 官方术语）
	fmt.Println("OfNonZero 数值:", stream.OfNonZero(1, 0, 2, 0, 3).ToSlice())
	a, b := 1, 2
	fmt.Println("OfNonZero 指针个数:",
		stream.OfNonZero(&a, nil, &b).Count()) // nil 是 *int 的零值，被过滤
}
