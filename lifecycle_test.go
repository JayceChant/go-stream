package stream

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JayceChant/go-stream/collector"
)

// lifecycle_test.go：Task 10 三项遗留（OnClose/Close、Cache、Unordered
// 流式合并）的语义验证。

// ---- OnClose / Close ----

func TestOnCloseAutoRelease(t *testing.T) {
	// 正常耗尽路径：求值结束自动触发恰好一次。
	var n int
	Of(1, 2, 3).OnClose(func() error { n++; return nil }).ToSlice()
	if n != 1 {
		t.Errorf("耗尽路径 release 调用 %d 次, 期望 1", n)
	}
}

func TestOnCloseShortCircuit(t *testing.T) {
	// 短路路径（First）：求值结束仍触发。
	var n int
	Of(1, 2, 3).OnClose(func() error { n++; return nil }).First()
	if n != 1 {
		t.Errorf("短路路径 release 调用 %d 次, 期望 1", n)
	}
}

func TestOnCloseErrPath(t *testing.T) {
	// 错误值路径（FromFunc 出错）：触发且错误槽为源错误。
	var n int
	s := FromFunc(func() (int, bool, error) { return 0, false, errors.New("src") }).
		OnClose(func() error { n++; return nil })
	s.ToSlice()
	if n != 1 {
		t.Errorf("错误路径 release 调用 %d 次, 期望 1", n)
	}
	if s.Err() == nil || s.Err().Error() != "src" {
		t.Errorf("Err() = %v, 期望源错误 src", s.Err())
	}
}

func TestOnCloseChainOrderAndFirstErr(t *testing.T) {
	// 多回调按注册序执行；出错记首错（不 panic）。
	var order []string
	s := Of(1).OnClose(func() error { order = append(order, "a"); return errors.New("ea") }).
		OnClose(func() error { order = append(order, "b"); return errors.New("eb") })
	s.ToSlice()
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("回调序 = %v, 期望 [a b]", order)
	}
	if err := s.Err(); err == nil || err.Error() != "ea" {
		t.Errorf("Err() = %v, 期望首错 ea", err)
	}
}

func TestOnClosePanicPath(t *testing.T) {
	// 用户回调 panic 的展开路径也触发（runClosers 由 defer 调用）。
	var n int
	s := Of(1).OnClose(func() error { n++; return nil })
	func() {
		defer func() { _ = recover() }()
		s.Peek(func(int) { panic("boom") }).ToSlice()
	}()
	if n != 1 {
		t.Errorf("panic 路径 release 调用 %d 次, 期望 1", n)
	}
}

func TestCloseExplicitIdempotent(t *testing.T) {
	// 显式 Close 幂等：重复调用不重复触发；未求值流也可关闭。
	var n int
	s := Of(1).OnClose(func() error { n++; return nil })
	if err := s.Close(); err != nil || n != 1 {
		t.Fatalf("Close() = %v, n = %d, 期望 nil/1", err, n)
	}
	_ = s.Close()
	if n != 1 {
		t.Errorf("重复 Close 后 n = %d, 期望仍为 1", n)
	}
}

func TestCloseReturnsFirstErr(t *testing.T) {
	s := Of(1).OnClose(func() error { return errors.New("cleanup") })
	if err := s.Close(); err == nil || err.Error() != "cleanup" {
		t.Fatalf("Close() = %v, 期望 cleanup", err)
	}
	if !errors.Is(s.Err(), s.Close()) { // 二次调用仍返回同一错误
		t.Errorf("重复 Close 返回值不一致: %v vs %v", s.Err(), s.Close())
	}
}

func TestOnCloseNilPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("OnClose(nil) 应 panic")
		}
	}()
	Of(1).OnClose(nil)
}

func TestOnCloseInheritedByDownstream(t *testing.T) {
	// 回调链沿中间操作继承：下游求值结束触发全链。
	var n int
	s := Of(1, 2, 3, 4).OnClose(func() error { n++; return nil })
	got := s.Filter(func(v int) bool { return v%2 == 0 }).Map(func(v int) int { return v * 10 }).ToSlice()
	if !slices.Equal(got, []int{20, 40}) {
		t.Fatalf("下游链结果 = %v", got)
	}
	if n != 1 {
		t.Errorf("继承链 release 调用 %d 次, 期望 1", n)
	}
}

func TestOnCloseConcatBothChains(t *testing.T) {
	// Concat 继承双方回调链：组合流求值结束一并触发（各恰好一次）。
	var a, b int
	c := Concat(
		Of(1).OnClose(func() error { a++; return nil }),
		Of(2).OnClose(func() error { b++; return nil }),
	)
	c.ToSlice()
	if a != 1 || b != 1 {
		t.Errorf("Concat 后回调次数 a=%d b=%d, 期望 1/1", a, b)
	}
}

func TestOnCloseParallelFiresOnce(t *testing.T) {
	// 并行求值：片 goroutine 重入 drive 不触发；收尾仅一次。
	var n atomic.Int32
	s := Range(0, 1000).Parallel(4).OnClose(func() error { n.Add(1); return nil })
	got := s.ToSlice()
	if int64(len(got)) != 1000 {
		t.Fatalf("并行 ToSlice 长度 %d, 期望 1000", len(got))
	}
	if n.Load() != 1 {
		t.Errorf("并行后 release 调用 %d 次, 期望 1", n.Load())
	}
}

// ---- Cache ----

func TestCacheReplay(t *testing.T) {
	// 上游只求值一次；两次消费结果一致；产物为一次性流。
	var evals int32
	items := []int{1, 2, 3}
	idx := 0
	up := FromFunc(func() (int, bool, error) {
		atomic.AddInt32(&evals, 1)
		if idx >= len(items) {
			return 0, false, nil
		}
		v := items[idx]
		idx++
		return v, true, nil
	})
	f := Cache(up)
	if n1 := f().Count(); n1 != 3 {
		t.Fatalf("首次 Count = %d, 期望 3", n1)
	}
	if n2 := f().Count(); n2 != 3 {
		t.Fatalf("重放 Count = %d, 期望 3", n2)
	}
	if got := atomic.LoadInt32(&evals); got != int32(len(items))+1 {
		t.Errorf("上游 next 调用 %d 次, 期望 %d（只求值一次：3 元素 + 1 次耗尽）", got, len(items)+1)
	}
	// 产物为一次性流：重复消费 panic
	s := f()
	s.ToSlice()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("产物重复消费应 panic")
			}
		}()
		s.ToSlice()
	}()
}

func TestCacheNotCalledUpstreamUsable(t *testing.T) {
	// 工厂从未被调用：原流仍可用（Cache 构造本身不消费）。
	s := Of(7)
	_ = Cache(s)
	if v, ok := s.First(); !ok || v != 7 {
		t.Errorf("First() = (%v, %v), 期望 (7, true)", v, ok)
	}
}

func TestCacheErrMemorized(t *testing.T) {
	// 物化期上游出错：首错记忆，此后每次调用返回携带错误的空流。
	wantErr := errors.New("io")
	f := Cache(FromFunc(func() (int, bool, error) { return 0, false, wantErr }))
	for i := 0; i < 2; i++ {
		s := f()
		if got := s.ToSlice(); len(got) != 0 {
			t.Errorf("第 %d 次 ToSlice = %v, 期望空", i+1, got)
		}
		if !errors.Is(s.Err(), wantErr) {
			t.Errorf("第 %d 次 Err() = %v, 期望 %v", i+1, s.Err(), wantErr)
		}
	}
}

// ---- Unordered / FromMap 特征位 ----

func TestFromMapUnorderedChars(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	if c := FromMap(m).chars; c&SpOrdered != 0 {
		t.Errorf("FromMap 特征位 = %b, 不应含 SpOrdered（Unordered 源）", c)
	}
}

func TestUnorderedClearsFlag(t *testing.T) {
	s := FromSlice([]int{1, 2, 3}).Unordered()
	if c := s.chars; c&SpOrdered != 0 {
		t.Errorf("Unordered 后特征位 = %b, SpOrdered 应清除", c)
	}
	if c := s.Sequential().chars; c&SpOrdered != 0 {
		t.Errorf("Sequential 后特征位 = %b, SpOrdered 不应被恢复", c)
	}
}

// ---- Unordered 流式合并 ----

func TestUnorderedStreamMergeToSlice(t *testing.T) {
	// 元素级流式合并：集合与串行一致（顺序不保证）。
	n := 1000
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	got := FromSlice(data).Parallel(4).Unordered().ToSlice()
	if len(got) != n {
		t.Fatalf("ToSlice 长度 %d, 期望 %d", len(got), n)
	}
	slices.Sort(got)
	if !slices.Equal(got, data) {
		t.Fatal("ToSlice 元素集合与串行不一致")
	}
}

func TestUnorderedStreamMergeCollect(t *testing.T) {
	// Collect 片级 Combiner 按完成序合并：分组结果与串行一致。
	n := 1000
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	got := FromSlice(data).Parallel(4).Unordered().
		Collect(collector.GroupingBy(func(v int) int { return v % 10 }, func(v int) int { return v }))
	if len(got) != 10 {
		t.Fatalf("分组数 %d, 期望 10", len(got))
	}
	for k, vs := range got {
		if len(vs) != n/10 {
			t.Fatalf("组 %d 大小 %d, 期望 %d", k, len(vs), n/10)
		}
		slices.Sort(vs)
		for i, v := range vs {
			if want := k + i*10; v != want {
				t.Fatalf("组 %d 第 %d 个 = %d, 期望 %d", k, i, v, want)
			}
		}
	}
}

func TestUnorderedStreamMergeForEachSum(t *testing.T) {
	n := 500
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	var sum atomic.Int64
	FromSlice(data).Parallel(4).Unordered().ForEach(func(v int) {
		sum.Add(int64(v))
	})
	var want int64
	for _, v := range data {
		want += int64(v)
	}
	if sum.Load() != want {
		t.Errorf("无序 ForEach 求和 = %d, 期望 %d", sum.Load(), want)
	}
}

func TestUnorderedStreamMergeMinMax(t *testing.T) {
	n := 1000
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	cmp := func(a, b int) int { return a - b }
	lo, ok1 := FromSlice(data).Parallel(4).Unordered().Min(cmp)
	hi, ok2 := FromSlice(data).Parallel(4).Unordered().Max(cmp)
	if !ok1 || lo != 0 {
		t.Errorf("Min = (%d, %v), 期望 (0, true)", lo, ok1)
	}
	if !ok2 || hi != n-1 {
		t.Errorf("Max = (%d, %v), 期望 (%d, true)", hi, ok2, n-1)
	}
}

func TestUnorderedStreamMergeStreamsEarly(t *testing.T) {
	// 确定性验证「先完成先推，不等待全部片」：后片（500..999）以
	// Peek 阻塞在 release 上模拟慢分片；若流式合并生效，快片元素在
	// release 前即达终端（否则等待全部片将超时失败）。
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	release := make(chan struct{})
	first := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		FromSlice(data).Parallel(2).Unordered().
			Peek(func(v int) {
				if v >= 500 {
					<-release // 慢分片：阻塞至终端收到快片首元素
				}
			}).
			ForEach(func(v int) {
				select {
				case first <- v:
				default: // 只记录首达
				}
			})
	}()
	select {
	case v := <-first:
		if v >= 500 {
			t.Errorf("首达元素 %d 来自慢分片, 期望快分片（<500）", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("流式合并未生效：终端在全部片完成前未收到快片元素")
	}
	close(release)
	<-done
}

func TestOrderedParallelUnchanged(t *testing.T) {
	// 有序流路径不受影响：仍按分片序保序合并。
	n := 1000
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	got := FromSlice(data).Parallel(4).ToSlice()
	if !slices.Equal(got, data) {
		t.Fatal("有序并行应保序")
	}
}
