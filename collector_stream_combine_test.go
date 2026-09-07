package stream

import (
	"testing"

	"github.com/JayceChant/go-stream/collector"
)

// collector_stream_combine_test.go：Task 20——组合收集器接入根包流的集成测试。

func TestGroupingByDownstreamOnStream(t *testing.T) {
	// 串行：分组计数
	got := Range(0, 100).AsStream().Collect(
		collector.GroupingByDownstream(func(v int) bool { return v%2 == 0 }, collector.Counting[int]()))
	if got[true] != 50 || got[false] != 50 {
		t.Errorf("分组计数 = %v", got)
	}

	// 并行：分片累积 + 组合并（GroupingByDownstream Combiner 路径）
	gotP := FromSlice(makeRange(0, 1000)).Parallel(4).Collect(
		collector.GroupingByDownstream(func(v int) int { return v % 3 }, collector.Summing[int]()))
	want := map[int]int{0: 166833, 1: 166167, 2: 166500}
	for k, v := range want {
		if gotP[k] != v {
			t.Errorf("并行分组求和[%d] = %d, 期望 %d", k, gotP[k], v)
		}
	}

	// 并行 Teeing：双下游各自分片合并
	gotT := FromSlice(makeRange(1, 101)).Parallel(4).Collect(
		collector.Teeing(collector.Summing[int](), collector.Counting[int](),
			func(s int, n int64) float64 { return float64(s) / float64(n) }))
	if gotT != 50.5 {
		t.Errorf("并行 Teeing 均值 = %v", gotT)
	}

	// PartitioningBy 并行
	gotPart := FromSlice(makeRange(0, 100)).Parallel(4).Collect(
		collector.PartitioningBySlice(func(v int) bool { return v < 50 }))
	if len(gotPart.True) != 50 || len(gotPart.False) != 50 {
		t.Errorf("并行分区 = %d/%d", len(gotPart.True), len(gotPart.False))
	}

	// MinBy 嵌入 GroupingByDownstream：每组最大值
	gotM := Of(1, 2, 3, 4, 5, 6).Collect(
		collector.GroupingByDownstream(func(v int) string {
			if v%2 == 0 {
				return "even"
			}
			return "odd"
		}, collector.MaxBy(func(a, b int) int { return a - b })))
	if gotM["even"] != 6 || gotM["odd"] != 5 {
		t.Errorf("分组最大 = %v", gotM)
	}
}

func makeRange(start, stop int) []int {
	out := make([]int, 0, stop-start)
	for v := start; v < stop; v++ {
		out = append(out, v)
	}
	return out
}
