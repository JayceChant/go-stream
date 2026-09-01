package stream

import "testing"

// testSplitterator 测试用源：嵌入 baseSplitterator 基座，仅实现推进语义。
type testSplitterator[T any] struct {
	baseSplitterator[T]
	items []T
	pos   int
}

func (t *testSplitterator[T]) TryAdvance(f func(T) bool) bool {
	if t.pos >= len(t.items) {
		return false
	}
	v := t.items[t.pos]
	t.pos++
	return f(v)
}

func (t *testSplitterator[T]) ForEachRemaining(f func(T) bool) {
	for t.TryAdvance(f) {
	}
}

func TestBaseSplitteratorEmbedding(t *testing.T) {
	sp := &testSplitterator[int]{
		estSize: 3, chars: SpSized | SpOrdered,
		items: []int{1, 2, 3},
	}

	// 编译期断言：嵌入基座后即满足 Splitterator 接口（组合生效）
	var _ Splitterator[int] = sp

	if got := sp.EstimateSize(); got != 3 {
		t.Errorf("EstimateSize() = %d, 期望 3", got)
	}
	if got := sp.Characteristics(); got != SpSized|SpOrdered {
		t.Errorf("Characteristics() = %d, 期望 %d", got, SpSized|SpOrdered)
	}
	if sp.TrySplit() != nil {
		t.Error("默认 TrySplit() 应返回 nil（串行不可分裂）")
	}

	// 推进语义
	var seen []int
	for sp.TryAdvance(func(v int) bool {
		seen = append(seen, v)
		return true
	}) {
	}
	if len(seen) != 3 {
		t.Errorf("TryAdvance 共推进 %d 个元素, 期望 3", len(seen))
	}

	// 短路语义：f 返回 false 时停止推进
	sp2 := &testSplitterator[int]{
		estSize: 3, chars: SpSized | SpOrdered,
		items: []int{1, 2, 3},
	}
	count := 0
	sp2.ForEachRemaining(func(v int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("短路后消费 %d 个元素, 期望 2", count)
	}
}
