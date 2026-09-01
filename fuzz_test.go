package stream

import (
	"slices"
	"testing"

	"github.com/JayceChant/go-stream/collector"
)

// fuzz_test.go：Task 11 Fuzz 测试——以随机输入锁定核心不变量。
//
// 运行方式：`go test` 执行种子用例；深度探索用
// `go test -fuzz=FuzzSplitSrcInvariant -fuzztime=10s`（逐目标运行）。
// 语料不随仓提交（testdata/ 不入库，种子即测试内数据）。
// 输入规模统一截断（≤256 元素 / 参数取模收敛），聚焦语义边界而非暴力。
// fuzz 参数仅允许基本类型与 []byte：整数序列以 []byte 供给再转 int。

// fuzzData 截断 fuzz 输入规模并克隆（避免与引擎内部共享底层数组）。
func fuzzData[T any](data []T, maxN int) []T {
	if len(data) > maxN {
		data = data[:maxN]
	}
	return slices.Clone(data)
}

// toInts 把 byte 序列转为 int 序列（fuzz 输入适配）。
func toInts(b []byte) []int {
	out := make([]int, len(b))
	for i, v := range b {
		out[i] = int(v)
	}
	return out
}

// FuzzSplitSrcInvariant 锁定分片不变量：子源并集==原集合且保序、
// 各子源非空、份数 ≤ n 且 ≥1；可分源（≥2 元素且 n≥2）至少 2 份。
// （该目标的设计动因：Task 11 审计发现递归深处不可再分子源被丢弃的 bug。）
func FuzzSplitSrcInvariant(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4}, 4)
	f.Add([]byte{1}, 4)
	f.Add([]byte{1, 2}, 8)
	f.Add([]byte{}, 2)
	f.Add([]byte{5, 6, 7}, 3)
	f.Add([]byte{9, 8, 7, 6, 5, 4}, 5)
	f.Fuzz(func(t *testing.T, raw []byte, n int) {
		data := fuzzData(raw, 256)
		if n < 0 {
			n = -n
		}
		n %= 16
		src := newSliceSp(data, SpSized|SpOrdered)
		parts := splitSrc(src, n)
		if len(parts) < 1 {
			t.Fatalf("splitSrc 恒应至少返回 1 份，得 0")
		}
		if n < 2 {
			if len(parts) != 1 {
				t.Fatalf("n=%d 无需再分，份数 = %d, 期望恰 1 份", n, len(parts))
			}
		} else {
			if len(parts) > n {
				t.Fatalf("份数 %d 超过 n=%d", len(parts), n)
			}
			if len(data) >= 2 && len(parts) < 2 {
				t.Fatalf("可分源（%d 元素）n=%d 仅 %d 份，应至少 2 份", len(data), n, len(parts))
			}
		}
		var merged []byte
		for i, p := range parts {
			sub := drainSp(p.(Splitterator[byte]))
			if len(data) > 0 && len(sub) == 0 {
				t.Fatalf("第 %d 份为空子源（原集合非空）", i)
			}
			merged = append(merged, sub...)
		}
		if !slices.Equal(merged, data) {
			t.Fatalf("子源并集 %v != 原集合 %v（丢失/失序）", merged, data)
		}
	})
}

// FuzzCollectingSinkBoundary 锁定物化终端边界：接受元素数恒为
// min(limit, count)（limit<0 不限），元素按序保全。
func FuzzCollectingSinkBoundary(f *testing.F) {
	f.Add(int64(8), int64(3), int64(5))
	f.Add(int64(-1), int64(-1), int64(10))
	f.Add(int64(4), int64(0), int64(3))
	f.Add(int64(100), int64(200), int64(50))
	f.Fuzz(func(t *testing.T, size, limit, count int64) {
		if size < -1 {
			size = -1
		}
		if size > 1<<16 {
			size = 1 << 16
		}
		if limit < -1 {
			limit = -1
		}
		if limit > 1<<16 {
			limit = 1 << 16
		}
		if count < 0 {
			count = 0
		}
		if count > 1<<16 {
			count = 1 << 16
		}
		c := &collectingSink[int]{limit: limit}
		c.Begin(size)
		for i := 0; i < int(count); i++ {
			c.Accept(i)
		}
		want := count
		if limit >= 0 && limit < want {
			want = limit
		}
		if int64(len(c.buf)) != want {
			t.Fatalf("接受 %d 个元素, 期望 min(limit=%d, count=%d)=%d", len(c.buf), limit, count, want)
		}
		for i, v := range c.buf {
			if v != i {
				t.Fatalf("第 %d 个元素 = %d, 失序", i, v)
			}
		}
	})
}

// FuzzPipelineEquivalence 锁定管道与参考实现等价：
// Filter(byte 域取模) → Map(迁 int) → Limit → Skip → 条件 Reverse。
func FuzzPipelineEquivalence(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6}, uint8(2), uint8(10), uint8(3), uint8(1), uint8(1))
	f.Add([]byte{}, uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add([]byte{255, 0, 7}, uint8(3), uint8(9), uint8(2), uint8(2), uint8(0))
	f.Fuzz(func(t *testing.T, raw []byte, fm, add, limit, skip, rev uint8) {
		data := fuzzData(raw, 256)
		mod := int(fm)%6 + 2
		wantLimit := int64(limit)
		wantSkip := int64(skip)
		doRev := rev%2 == 1

		pipeline := FromSlice(slices.Clone(data)).
			Filter(func(v byte) bool { return int(v)%mod != 0 }).
			Map(func(v byte) int { return int(v) + int(add) }).
			Limit(wantLimit).
			Skip(wantSkip)
		if doRev {
			pipeline = pipeline.Reverse()
		}
		got := pipeline.ToSlice()

		// 参考实现（手写切片运算，与管道语义逐步对齐）。
		ref := []int{}
		for _, v := range data {
			if int(v)%mod != 0 {
				ref = append(ref, int(v)+int(add))
			}
		}
		if int64(len(ref)) > wantLimit {
			ref = ref[:wantLimit]
		}
		if int64(len(ref)) > wantSkip {
			ref = ref[wantSkip:]
		} else {
			ref = nil
		}
		if doRev {
			slices.Reverse(ref)
		}
		if !slices.Equal(got, ref) {
			t.Fatalf("管道 %v != 参考 %v（mod=%d add=%d limit=%d skip=%d rev=%v）",
				got, ref, mod, add, wantLimit, wantSkip, doRev)
		}
	})
}

// FuzzParallelEquivalence 锁定并行与串行等价：有序并行逐元素一致
// （保序）、无序并行集合一致、Count 一致、Collect 分组计数一致。
func FuzzParallelEquivalence(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7}, uint8(4))
	f.Add([]byte{42}, uint8(8))
	f.Add([]byte{}, uint8(2))
	f.Add([]byte{255, 254, 253, 252, 251}, uint8(3))
	f.Add([]byte{9, 9, 9, 1, 1, 2}, uint8(5))
	f.Fuzz(func(t *testing.T, raw []byte, nRaw uint8) {
		data := toInts(fuzzData(raw, 256))
		n := 2 + int(nRaw)%4 // 2..5

		// 有序并行：分片序回放保序，逐元素等于串行。
		if got := FromSlice(slices.Clone(data)).Parallel(n).ToSlice(); !slices.Equal(got, data) {
			t.Fatalf("Parallel(%d) ToSlice = %v, 期望 %v（丢失/失序）", n, got, data)
		}
		// Count 一致。
		if c := FromSlice(slices.Clone(data)).Parallel(n).Count(); c != int64(len(data)) {
			t.Fatalf("Parallel(%d) Count = %d, 期望 %d", n, c, len(data))
		}
		// 无序流式合并：集合一致（顺序不保证）。
		gotU := FromSlice(slices.Clone(data)).Parallel(n).Unordered().ToSlice()
		wantU := slices.Clone(data)
		slices.Sort(gotU)
		slices.Sort(wantU)
		if !slices.Equal(gotU, wantU) {
			t.Fatalf("Unordered Parallel(%d) 集合 %v != %v", n, gotU, wantU)
		}
		// Collect 分组：各组元素计数与串行一致。
		group := func(v int) int { return v % 7 }
		g1 := FromSlice(slices.Clone(data)).Parallel(n).
			Collect(collector.GroupingBy(group, func(v int) int { return v }))
		g2 := FromSlice(slices.Clone(data)).
			Collect(collector.GroupingBy(group, func(v int) int { return v }))
		if len(g1) != len(g2) {
			t.Fatalf("分组键数 %d != %d", len(g1), len(g2))
		}
		for k, vs := range g2 {
			if g1[k] == nil || len(g1[k]) != len(vs) {
				t.Fatalf("组 %d 计数不一致：并行 %d vs 串行 %d", k, len(g1[k]), len(vs))
			}
		}
	})
}

// FuzzZipShortest 锁定 Zip 取短语义：长度 = min(len(a), len(b))，
// 第 i 对恒为 (a[i], b[i])。
func FuzzZipShortest(f *testing.F) {
	f.Add([]byte{1, 2, 3}, []byte{10, 20, 30})
	f.Add([]byte{1}, []byte{10, 20})
	f.Add([]byte{}, []byte{1, 2})
	f.Add([]byte{7, 8}, []byte{})
	f.Fuzz(func(t *testing.T, rawA, rawB []byte) {
		a := fuzzData(rawA, 256)
		b := fuzzData(rawB, 256)
		got := Of(slices.Clone(a)...).
			Zip(Of(slices.Clone(b)...), func(x, y byte) KV[byte, byte] { return KV[byte, byte]{Key: x, Value: y} }).
			ToSlice()
		wantLen := min(len(a), len(b))
		if len(got) != wantLen {
			t.Fatalf("Zip 长度 = %d, 期望 min(%d, %d)=%d", len(got), len(a), len(b), wantLen)
		}
		for i, kv := range got {
			if kv.Key != a[i] || kv.Value != b[i] {
				t.Fatalf("第 %d 对 = (%d, %d), 期望 (%d, %d)", i, kv.Key, kv.Value, a[i], b[i])
			}
		}
	})
}

// FuzzChunkEnumerate 锁定 Chunk 定长分组（尾组可不足）与 Enumerate 索引配对。
func FuzzChunkEnumerate(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5}, uint8(2))
	f.Add([]byte{}, uint8(3))
	f.Add([]byte{9}, uint8(1))
	f.Add([]byte{4, 5, 6}, uint8(5))
	f.Fuzz(func(t *testing.T, raw []byte, nRaw uint8) {
		data := fuzzData(raw, 256)
		n := 1 + int(nRaw)%8

		// Chunk：定长分组，尾组可不足 n。
		chunks := Chunk(FromSlice(slices.Clone(data)), n).ToSlice()
		var flat []byte
		for i, c := range chunks {
			if len(c) == 0 || len(c) > n {
				t.Fatalf("第 %d 组长度 %d 非法（n=%d）", i, len(c), n)
			}
			if i < len(chunks)-1 && len(c) != n {
				t.Fatalf("非尾组 %d 长度 %d != n=%d", i, len(c), n)
			}
			flat = append(flat, c...)
		}
		if !slices.Equal(flat, data) {
			t.Fatalf("Chunk 展平 %v != 原序列 %v", flat, data)
		}

		// Enumerate：索引从 0 起配对。
		enum := Enumerate(FromSlice(slices.Clone(data))).ToSlice()
		if len(enum) != len(data) {
			t.Fatalf("Enumerate 长度 %d != %d", len(enum), len(data))
		}
		for i, kv := range enum {
			if kv.Key != i || kv.Value != data[i] {
				t.Fatalf("Enumerate[%d] = (%d, %v), 期望 (%d, %v)", i, kv.Key, kv.Value, i, data[i])
			}
		}
	})
}

// FuzzCacheReplayEquivalence 锁定可重放工厂：任意随机序列下，
// 上游只求值一次，多次重放结果与原序列一致。
func FuzzCacheReplayEquivalence(f *testing.F) {
	f.Add([]byte{3, 1, 2})
	f.Add([]byte{})
	f.Add([]byte{251, 0, 5, 251})
	f.Fuzz(func(t *testing.T, raw []byte) {
		data := fuzzData(raw, 256)
		evals := 0
		src := FromSlice(data).Peek(func(byte) { evals++ })
		factory := Cache(src)
		for round := range 3 {
			if got := factory().ToSlice(); !slices.Equal(got, data) {
				t.Fatalf("第 %d 轮重放 = %v, 期望 %v", round+1, got, data)
			}
		}
		if evals != len(data) {
			t.Fatalf("上游求值 %d 次, 期望恰好 %d（只求值一次）", evals, len(data))
		}
	})
}
