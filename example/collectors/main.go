// Package main 演示 Collector 汇聚：预置收集器族与自定义 Collector。
//
// 运行：go -C example run ./collectors（example 为独立模块，不影响库的测试与覆盖率）
//
// Collect(c) 是终止操作：把流元素经 Supplier/Accumulator 累积，
// Finisher 收尾返回最终结果；Combiner 供并行求值按分片合并。
// 预置收集器位于子包 collector（stream/collector）。
package main

import (
	"fmt"
	"slices"

	"github.com/JayceChant/go-stream"
	"github.com/JayceChant/go-stream/collector"
)

// order 是贯穿本示例的业务模型。
type order struct {
	id     int
	user   string
	amount float64
	region string
}

var orders = []order{
	{1, "Alice", 120.5, "north"},
	{2, "Bob", 89.0, "south"},
	{3, "Alice", 45.0, "north"},
	{4, "Carol", 230.0, "south"},
	{5, "Bob", 60.5, "north"},
}

func main() {
	src := func() *stream.Stream[order] { return stream.FromSlice(orders) }

	// ---------- 1. ToSet：去重集合 ----------
	// 先把用户名收集为流，再由 ToSet 汇聚
	users := src().Map(func(o order) string { return o.user }).
		Collect(collector.ToSet[string]())
	fmt.Println("下单用户:", sortedKeys(users))

	// ---------- 2. ToMap：键冲突 last-wins ----------
	// 每个用户只保留最后一笔订单的金额
	lastAmount := src().Collect(collector.ToMap(
		func(o order) string { return o.user },
		func(o order) float64 { return o.amount },
	))
	fmt.Println("每人最后一单:", lastAmount)

	// ---------- 3. ToMapMerge：键冲突自定义合并 ----------
	// 每个用户的累计消费额（merge 收到旧值与新值）
	totalByUser := src().Collect(collector.ToMapMerge(
		func(o order) string { return o.user },
		func(o order) float64 { return o.amount },
		func(oldV, newV float64) float64 { return oldV + newV },
	))
	fmt.Println("每人累计消费:", totalByUser)

	// ---------- 4. GroupingBy：分组（组内保持遇序） ----------
	ordersByRegion := src().Collect(collector.GroupingBy(
		func(o order) string { return o.region },
		func(o order) int { return o.id },
	))
	fmt.Println("按区域分组订单号:", ordersByRegion)

	// ---------- 5. Joining：字符串拼接 ----------
	idList := src().Collect(collector.Joining(
		func(o order) string { return fmt.Sprintf("#%d", o.id) },
		", ",
	))
	fmt.Println("订单号清单:", idList)

	// ---------- 6. Counting / Reducing ----------
	fmt.Println("订单总数:", src().Collect(collector.Counting[order]()))
	maxAmount := src().
		Map(func(o order) float64 { return o.amount }).
		Collect(collector.Reducing(0.0, func(a, b float64) float64 { return max(a, b) }))
	fmt.Println("最大单额:", maxAmount)

	// ---------- 7. Mapping：收集器组合子 ----------
	// 先变换元素，再交给下游收集器（对齐 Java Collectors.mapping）
	regionTotal := src().Collect(collector.Mapping(
		func(o order) stream.KV[string, float64] {
			return stream.KV[string, float64]{Key: o.region, Value: o.amount}
		},
		collector.ToMapMerge(
			func(kv stream.KV[string, float64]) string { return kv.Key },
			func(kv stream.KV[string, float64]) float64 { return kv.Value },
			func(oldV, newV float64) float64 { return oldV + newV },
		),
	))
	fmt.Println("每区域累计:", regionTotal)

	// ---------- 8. Summing：数值求和（根包，依赖 Number 约束） ----------
	fmt.Println("流水总计:", src().
		Map(func(o order) float64 { return o.amount }).
		Collect(stream.Summing[float64]()))

	// ---------- 9. 自定义 Collector ----------
	// TopN：保留金额最大的 N 笔订单（Finisher 收尾排序输出）。
	top2 := src().Collect(topN(2, func(o order) float64 { return o.amount }))
	for i, o := range top2 {
		fmt.Printf("Top%d: #%d %s %.1f\n", i+1, o.id, o.user, o.amount)
	}
}

// topN 构造"取指标最大的前 n 个元素"的收集器：Accumulator 累积，
// 溢出时截断，Finisher 收尾排序（降序取前 n）。Combiner 供并行合并。
func topN[T any](n int, key func(T) float64) collector.Collector[T, *[]T, []T] {
	return collector.Collector[T, *[]T, []T]{
		Supplier: func() *[]T { return &[]T{} },
		Accumulator: func(a *[]T, v T) {
			*a = append(*a, v)
			if len(*a) > 2*n { // 防缓冲膨胀：溢出即截断（真实场景可用 container/heap）
				*a = bestN(*a, n, key)
			}
		},
		Combiner: func(a, b *[]T) *[]T {
			*a = append(*a, *b...)
			*a = bestN(*a, n, key)
			return a
		},
		Finisher: func(a *[]T) []T {
			return bestN(*a, n, key)
		},
	}
}

// bestN 返回按 key 降序前 n 个元素（副本）。
func bestN[T any](xs []T, n int, key func(T) float64) []T {
	out := slices.Clone(xs)
	slices.SortFunc(out, func(a, b T) int {
		switch {
		case key(a) > key(b):
			return -1
		case key(a) < key(b):
			return 1
		default:
			return 0
		}
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// sortedKeys 把 set 转为有序切片，保证打印输出稳定。
func sortedKeys(set map[string]struct{}) []string {
	out := mapsKeys(set)
	slices.Sort(out)
	return out
}

// mapsKeys 提取 map 键（配合 slices.Sort 生成稳定输出）。
func mapsKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
