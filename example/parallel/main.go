// Package main 演示并行求值：Parallel(n)/Sequential/Unordered、保序合并与自动降级。
//
// 运行：go -C example run ./parallel（example 为独立模块，不影响库的测试与覆盖率）
//
// 要点：
//   - Parallel(n) 是中间操作：声明后续求值最多以 n 个分片并行；
//     可分源（slice/range）才会真正分片
//   - 有序流按分片序合并，结果与串行完全一致（保序）
//   - Unordered 声明不依赖相遇序，并行下分片先完成先推（降延迟）
//   - 含物化有状态算子（Sorted/DistinctBy/...）或短路终端时自动降级串行，
//     正确性优先，无需用户干预
package main

import (
	"fmt"
	"slices"

	"github.com/JayceChant/go-stream"
	"github.com/JayceChant/go-stream/collector"
)

// cpuHeavy 模拟 CPU 密集计算（并行收益场景）。
func cpuHeavy(n int) int {
	var h int
	for i := range 2000 {
		h = (h + n*i) % 1000003
	}
	return h
}

func main() {
	// ---------- 1. 并行求值，结果与串行一致（保序） ----------
	serial := stream.Range(0, 1000).
		Filter(func(n int) bool { return n%3 == 0 }).
		Map(cpuHeavy).
		ToSlice()

	parallel := stream.Range(0, 1000).
		Parallel(4). // 声明 4 分片并行
		Filter(func(n int) bool { return n%3 == 0 }).
		Map(cpuHeavy).
		ToSlice()

	fmt.Println("并行与串行结果一致:", slices.Equal(serial, parallel),
		fmt.Sprintf("(前5: %v)", parallel[:5]))

	// ---------- 2. 并行 + Collect：Combiner 分片合并 ----------
	grouped := stream.Range(0, 1000).
		Parallel(4).
		Collect(collector.GroupingBy(
			func(n int) int { return n % 10 },
			func(n int) int { return n },
		))
	fmt.Println("并行分组：桶数 =", len(grouped), " 0号桶数量 =", len(grouped[0]))

	total := stream.Range(0, 1000).
		Parallel(4).
		Map(cpuHeavy).
		Collect(stream.Summing[int]())
	fmt.Println("并行求和:", total)

	// ---------- 3. Unordered：不依赖顺序时先完成先推 ----------
	// 元素集合与串行一致，顺序不保证（本就是无序语义）。
	set := stream.Range(0, 1000).
		Parallel(4).
		Unordered(). // 声明后续求值不需要保序
		Map(cpuHeavy).
		Collect(collector.ToSet[int]())
	fmt.Println("无序并行去重后集合大小:", len(set))

	// ---------- 4. Sequential 还原串行 ----------
	count := stream.Range(0, 100).
		Parallel(4).  // 先声明并行
		Sequential(). // 再还原串行（抵消 Parallel）
		Count()
	fmt.Println("Sequential 抵消后 Count:", count)

	// ---------- 5. 自动降级：正确性优先 ----------
	// Sorted（物化有状态算子）之后并行不可行，引擎自动降级串行求值，
	// 用户无需关心；结果依然正确。
	sortedTop := stream.Range(0, 1000).
		Parallel(4).
		Sorted(func(a, b int) int { return b - a }). // 物化算子：触发降级
		Limit(3).
		ToSlice()
	fmt.Println("Sorted+Limit 降级串行仍正确:", sortedTop)
}
