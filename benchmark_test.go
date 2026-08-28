package stream

// benchmark_test.go：管道 vs 手写 for 循环性能对比。
//
// 目标（spec「质量保障」）：Filter+Map+ToSlice 相对手写 for 额外开销 <3x。

import (
	"fmt"
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

// sinkInt 防止手写循环被死代码消除。
var sinkInt int

func BenchmarkPipelineIntVsManual(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("Pipeline_%d", n), func(b *testing.B) { benchPipelineInt(b, n) })
		b.Run(fmt.Sprintf("Manual_%d", n), func(b *testing.B) { benchManualInt(b, n) })
	}
}
