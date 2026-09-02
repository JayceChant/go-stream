package stream

import "testing"

func TestLimit(t *testing.T) {
	if got := collectViaProbe(Of(1, 2, 3, 4).Limit(2)); len(got) != 2 || got[1] != 2 {
		t.Errorf("Limit(2) = %v, 期望 [1 2]", got)
	}
	// n 超界：全量
	if got := collectViaProbe(Of(1, 2).Limit(10)); len(got) != 2 {
		t.Errorf("Limit(10) = %v, 期望 2 个", got)
	}
	// n=0：空
	if got := collectViaProbe(Of(1, 2).Limit(0)); len(got) != 0 {
		t.Errorf("Limit(0) = %v, 期望空", got)
	}
	// 无限源终止
	if got := collectViaProbe(Generate(func() int { return 7 }).Limit(3)); len(got) != 3 {
		t.Errorf("无限源 Limit(3) = %v, 期望 3 个", got)
	}
	// 负数 panic
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Limit(-1) 应 panic")
			}
		}()
		Of(1).Limit(-1)
	}()
}

func TestSkip(t *testing.T) {
	if got := collectViaProbe(Of(1, 2, 3, 4).Skip(2)); len(got) != 2 || got[0] != 3 {
		t.Errorf("Skip(2) = %v, 期望 [3 4]", got)
	}
	// 超界：空
	if got := collectViaProbe(Of(1, 2).Skip(10)); len(got) != 0 {
		t.Errorf("Skip(10) = %v, 期望空", got)
	}
	// n=0：全量
	if got := collectViaProbe(Of(1, 2).Skip(0)); len(got) != 2 {
		t.Errorf("Skip(0) = %v, 期望 2 个", got)
	}
	// n=0：恒等返回原流（同一对象），原流未被标记 consumed 仍可继续链接
	s := Of(1, 2, 3)
	if id := s.Skip(0); id != s {
		t.Error("Skip(0) 应恒等返回原流")
	}
	if got := collectViaProbe(s.Map(func(v int) int { return v * 10 })); len(got) != 3 || got[0] != 10 {
		t.Errorf("Skip(0) 后原流应仍可链接，got = %v", got)
	}
	// n=0：不物化——短路终端（First）只拉首元素，源不被驱动到耗尽
	pulls := 0
	if v, ok := Generate(func() int { pulls++; return pulls }).Skip(0).First(); !ok || v != 1 {
		t.Errorf("Skip(0)+First = (%v, %v), 期望 (1, true)", v, ok)
	}
	if pulls != 1 {
		t.Errorf("Skip(0)+First 应只拉 1 个元素，实际拉了 %d 个（物化未豁免）", pulls)
	}
}

type rec struct {
	key  int
	name string
}

func TestSortedStable(t *testing.T) {
	in := []rec{{2, "b1"}, {1, "a1"}, {2, "b2"}, {1, "a2"}}
	got := collectViaProbe(FromSlice(in).Sorted(func(a, b rec) int {
		return a.key - b.key
	}))
	// 稳定排序：key 相同时保持原相对顺序（a1 在 a2 前、b1 在 b2 前）
	want := []rec{{1, "a1"}, {1, "a2"}, {2, "b1"}, {2, "b2"}}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SpSorted[%d] = %+v, 期望 %+v", i, got[i], want[i])
		}
	}
	// 不影响输入切片
	if in[0] != (rec{2, "b1"}) {
		t.Error("SpSorted 不应修改输入切片")
	}
}

func TestDistinctByAndDistinct(t *testing.T) {
	type item struct {
		id  int
		val string
	}
	got := collectViaProbe(Of(item{1, "a"}, item{2, "b"}, item{1, "c"}).
		DistinctBy(func(x item) any { return x.id }))
	if len(got) != 2 || got[0].val != "a" || got[1].val != "b" {
		t.Errorf("DistinctBy = %v, 期望保首见 [a b]", got)
	}
	// 包级 Distinct
	got2 := collectViaProbe(Distinct(Of(3, 1, 3, 2, 1)))
	if len(got2) != 3 || got2[0] != 3 || got2[2] != 2 {
		t.Errorf("SpDistinct = %v, 期望 [3 1 2]", got2)
	}
}

func TestReverse(t *testing.T) {
	if got := collectViaProbe(Of(1, 2, 3).Reverse()); len(got) != 3 || got[0] != 3 {
		t.Errorf("Reverse = %v, 期望 [3 2 1]", got)
	}
	// 不影响输入切片
	in := []int{1, 2}
	collectViaProbe(FromSlice(in).Reverse())
	if in[0] != 1 || in[1] != 2 {
		t.Error("Reverse 不应修改输入切片")
	}
}

func TestScan(t *testing.T) {
	// 前缀和（含初值 0）：[0 1 3 6]
	got := collectViaProbe(Of(1, 2, 3).Scan(0, func(acc, v int) int { return acc + v }))
	if len(got) != 4 || got[0] != 0 || got[3] != 6 {
		t.Errorf("Scan = %v, 期望 [0 1 3 6]", got)
	}
}

func TestChunk(t *testing.T) {
	// 尾块不足 n
	got := collectViaProbe(Chunk(Of(1, 2, 3, 4, 5), 2))
	if len(got) != 3 {
		t.Fatalf("Chunk(2) 组数 = %d, 期望 3", len(got))
	}
	if len(got[2]) != 1 || got[2][0] != 5 {
		t.Errorf("尾块 = %v, 期望 [5]", got[2])
	}
	// 无限源：仅构造（不求值）不 panic；配合 Limit 后求值正常
	inf := Chunk(Generate(func() int { return 1 }), 3)
	if inf == nil {
		t.Fatal("无限源分块构造失败")
	}
	got3 := collectViaProbe(Chunk(Generate(func() int { return 1 }).Limit(4), 3))
	if len(got3) != 2 || len(got3[1]) != 1 {
		t.Errorf("无限源分块 = %v", got3)
	}
	// n<=0 panic
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Chunk(0) 应 panic")
			}
		}()
		Chunk(Of(1), 0)
	}()
}

func TestEnumerate(t *testing.T) {
	got := collectViaProbe(Enumerate(Of("a", "b")))
	if len(got) != 2 || got[0].Key != 0 || got[0].Value != "a" || got[1].Key != 1 {
		t.Errorf("Enumerate = %v", got)
	}
}
