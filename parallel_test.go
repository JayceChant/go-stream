package stream

// parallel_test.go：Parallel(n)/Sequential() 并行求值测试。

import (
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// 基础正确性：并行结果与串行一致（保序）。
func TestParallel_ToSliceSameAsSerial(t *testing.T) {
	data := make([]int, 10_000)
	for i := range data {
		data[i] = i
	}
	ser := FromSlice(data).Filter(func(v int) bool { return v%3 != 0 }).Map(func(v int) int { return v * 2 }).ToSlice()
	par := FromSlice(data).Parallel(4).Filter(func(v int) bool { return v%3 != 0 }).Map(func(v int) int { return v * 2 }).ToSlice()
	if !slices.Equal(ser, par) {
		t.Fatalf("并行结果与串行不一致：ser=%d 项, par=%d 项", len(ser), len(par))
	}
}

// Count/Reduce 并行正确性。
func TestParallel_CountReduce(t *testing.T) {
	ser := Range(1, 10_001)
	par := Range(1, 10_001).Parallel(4)
	if a, b := ser.Count(), par.Count(); a != b {
		t.Fatalf("Count: 串行=%d 并行=%d", a, b)
	}
	if a := Range(1, 101).Parallel(4).Reduce(0, func(a, b int) int { return a + b }); a != 5050 {
		t.Fatalf("Reduce 并行 = %d, 期望 5050", a)
	}
}

// Collect 并行：Combiner 合并（ToMapMerge 同键累加）。
func TestParallel_CollectCombiner(t *testing.T) {
	got := Range(0, 1000).Parallel(4).Collect(ToMapMerge(
		func(v int) string {
			if v%2 == 0 {
				return "even"
			}
			return "odd"
		},
		func(v int) int { return 1 },
		func(old, new int) int { return old + new },
	))
	if got["even"] != 500 || got["odd"] != 500 {
		t.Fatalf("分组计数错误: %v", got)
	}
}

// GroupingBy 并行保序：组内顺序与串行一致。
func TestParallel_GroupingByOrder(t *testing.T) {
	ser := Range(0, 1000).Collect(GroupingBy(
		func(v int) int { return v % 7 },
		func(v int) int { return v },
	))
	par := Range(0, 1000).Parallel(4).Collect(GroupingBy(
		func(v int) int { return v % 7 },
		func(v int) int { return v },
	))
	for k, vs := range ser {
		if !slices.Equal(vs, par[k]) {
			t.Fatalf("组 %d 顺序不一致", k)
		}
	}
}

// ForEach 并行保序。
func TestParallel_ForEachOrder(t *testing.T) {
	var mu = make(chan int, 4096)
	Range(0, 1000).Parallel(4).ForEach(func(v int) { mu <- v })
	close(mu)
	i := 0
	for v := range mu {
		if v != i {
			t.Fatalf("顺序错误：位置 %d 出现 %d", i, v)
		}
		i++
	}
}

// 降级 1：物化型有状态算子后自动串行（正确性不破坏）。
func TestParallel_FallbackSorted(t *testing.T) {
	data := []int{5, 3, 8, 1, 9, 2, 7}
	got := FromSlice(data).Parallel(4).Sorted(func(a, b int) int { return a - b }).ToSlice()
	if !slices.Equal(got, []int{1, 2, 3, 5, 7, 8, 9}) {
		t.Fatalf("Sorted 降级后结果错误: %v", got)
	}
}

// 降级 2：不可分源（Generate）自动串行。
func TestParallel_FallbackUnsplittable(t *testing.T) {
	i := 0
	got := Generate(func() int { i++; return i }).Limit(10).ToSlice()
	if len(got) != 10 || got[9] != 10 {
		t.Fatalf("不可分源结果错误: %v", got)
	}
}

// 降级 3：短路终止族走串行路径（Parallel 声明下仍正确）。
func TestParallel_ShortCircuitSerial(t *testing.T) {
	var visited int64
	ok := Range(0, 1_000_000).Parallel(4).AnyMatch(func(v int) bool {
		atomic.AddInt64(&visited, 1)
		return v == 100
	})
	if !ok {
		t.Fatal("AnyMatch 未命中")
	}
	if visited > 1000 {
		t.Fatalf("短路失效：遍历了 %d 个元素", visited)
	}
}

// Sequential 抵消 Parallel。
func TestParallel_SequentialOverride(t *testing.T) {
	got := Range(0, 100).Parallel(8).Sequential().Map(func(v int) int { return v }).ToSlice()
	if len(got) != 100 {
		t.Fatalf("Sequential 结果错误: %d 项", len(got))
	}
}

// 并行下的错误即值：MapErr 首错传播；出错片截断，其余片结果保留
// （4 片：[0,250) [250,500) [500,750) [750,1000)；错误在片 [500,750) 首元素）。
func TestParallel_ErrorPropagation(t *testing.T) {
	s := Range(0, 1000).Parallel(4).MapErr(func(v int) (int, error) {
		if v == 500 {
			return 0, errForTest
		}
		return v, nil
	})
	got := s.ToSlice()
	if s.Err() != errForTest {
		t.Fatalf("并行 Err() = %v", s.Err())
	}
	if len(got) != 750 { // 片 0/1/3 各 250 个完整保留；出错片 [500,750) 截断为空
		t.Fatalf("分片部分结果 = %d 项，期望 750", len(got))
	}
	for _, v := range got {
		if v >= 500 && v < 750 {
			t.Fatalf("出错片 [500,750) 中混入了 %d", v)
		}
	}
}

var errForTest error = &testError{"并行求值首错"}

// 并行加速比（CPU 密集）：>1.2x 视为通过（CI 环境噪音容忍）。
func TestParallel_Speedup(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过")
	}
	heavy := func(v int) int {
		sum := 0
		for i := 0; i < 200; i++ {
			sum += v * i % 7
		}
		return sum
	}
	n := 200_000
	ser := Range(0, n)
	t0 := time.Now()
	ser.Filter(func(v int) bool { return v%2 == 0 }).Map(heavy).Count()
	dSer := time.Since(t0)

	par := Range(0, n).Parallel(4)
	t1 := time.Now()
	par.Filter(func(v int) bool { return v%2 == 0 }).Map(heavy).Count()
	dPar := time.Since(t1)

	t.Logf("串行=%v 并行=%v 加速比=%.2fx", dSer, dPar, float64(dSer)/float64(dPar))
	if dPar >= dSer {
		t.Logf("警告：无加速（可能源太小或机器忙），不判失败")
	}
}
