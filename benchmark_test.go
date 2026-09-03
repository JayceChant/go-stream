package stream

// benchmark_test.go：管道 vs 手写 for 循环性能对比。
//
// 目标（spec「质量保障」）：Filter+Map+ToSlice 相对手写 for 额外开销 <3x。

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
)

var benchSizes = []int{100, 10_000, 1_000_000}

func makeBenchData(n int) []int {
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	return data
}

// 管道：Filter + Map + ToSlice。
func benchPipeline(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FromSlice(data).
			Filter(func(v int) bool { return v%2 == 0 }).
			Map(strconv.Itoa).
			ToSlice()
	}
}

// 手写 for 循环：等价语义。
func benchManual(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]string, 0, len(data)/2)
		for _, v := range data {
			if v%2 == 0 {
				out = append(out, strconv.Itoa(v))
			}
		}
		_ = out
	}
}

func BenchmarkPipelineVsManual(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("Pipeline_%d", n), func(b *testing.B) { benchPipeline(b, n) })
		b.Run(fmt.Sprintf("Manual_%d", n), func(b *testing.B) { benchManual(b, n) })
	}
}

// Top-K 场景（README「实现对比」同款）：筛正数金额 → 降序排序 → 取前三 →
// 格式化为价格字符串。有状态算子（Sorted/Limit）前后各有无状态操作，
// 手写等价实现被迫拆成两个循环加一次就地排序。
func benchPipelineTopK(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = FromSlice(data).
			Filter(func(v int) bool { return v > 0 }).
			Sorted(func(x, y int) int { return y - x }).
			Limit(3).
			Map(func(v int) string { return fmt.Sprintf("$%d", v) }).
			ToSlice()
	}
}

// Top-K 手写等价实现：收集正数到全新切片（append 构建，源不动，
// 与管道 collectingSink 物化对称）→ 就地稳定排序（与管道 Sorted 的
// 稳定排序契约一致）→ 取前三格式化。
func benchManualTopK(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		amounts := make([]int, 0, len(data))
		for _, v := range data {
			if v > 0 {
				amounts = append(amounts, v)
			}
		}
		slices.SortStableFunc(amounts, func(x, y int) int { return y - x })
		top := make([]string, 0, 3)
		for i, v := range amounts {
			if i >= 3 {
				break
			}
			top = append(top, fmt.Sprintf("$%d", v))
		}
		_ = top
	}
}

func BenchmarkTopKVsManual(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("Pipeline_%d", n), func(b *testing.B) { benchPipelineTopK(b, n) })
		b.Run(fmt.Sprintf("Manual_%d", n), func(b *testing.B) { benchManualTopK(b, n) })
	}
}

// 附加：整条链纯数值（无分配）场景，考察引擎裸开销。
func benchPipelineInt(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FromSlice(data).
			Filter(func(v int) bool { return v%2 == 0 }).
			Map(func(v int) int { return v * 2 }).
			Count()
	}
}

func benchManualInt(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var acc int
		for _, v := range data {
			if v%2 == 0 {
				acc += v * 2
			}
		}
		sinkInt = acc
	}
}

// sinkInt/sinkInt64 防止手写循环被死代码消除。
var (
	sinkInt   int
	sinkInt64 int64
)

func BenchmarkPipelineIntVsManual(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("Pipeline_%d", n), func(b *testing.B) { benchPipelineInt(b, n) })
		b.Run(fmt.Sprintf("Manual_%d", n), func(b *testing.B) { benchManualInt(b, n) })
	}
}

// DistinctBy 键类型对比：类型化键（K=int，map[int] 零装箱）vs any 键
// （K 推断为 any，逐元素装箱）vs 手写 map[int] 循环基线。
// 键取 v%1000，考察重复键场景下的去重开销。
func benchDistinctTyped(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sinkInt64 = FromSlice(data).DistinctBy(func(v int) int { return v % 1000 }).Count()
	}
}

func benchDistinctAny(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sinkInt64 = FromSlice(data).DistinctBy(func(v int) any { return v % 1000 }).Count()
	}
}

func benchDistinctManual(b *testing.B, n int) {
	data := makeBenchData(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		seen := make(map[int]struct{}, 1000)
		cnt := 0
		for _, v := range data {
			if _, dup := seen[v%1000]; !dup {
				seen[v%1000] = struct{}{}
				cnt++
			}
		}
		sinkInt = cnt
	}
}

func BenchmarkDistinctByKeyType(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("Typed_%d", n), func(b *testing.B) { benchDistinctTyped(b, n) })
		b.Run(fmt.Sprintf("Any_%d", n), func(b *testing.B) { benchDistinctAny(b, n) })
		b.Run(fmt.Sprintf("Manual_%d", n), func(b *testing.B) { benchDistinctManual(b, n) })
	}
}
