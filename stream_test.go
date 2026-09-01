package stream

import (
	"errors"
	"testing"
)

func TestSinkFunc(t *testing.T) {
	var got []int
	s := sinkFunc[int](func(v int) bool {
		got = append(got, v)
		return len(got) < 2
	})

	s.Begin(3) // 空实现，不应 panic
	if !s.Accept(1) {
		t.Error("第 1 次 Accept 应返回 true")
	}
	if s.Accept(2) {
		t.Error("第 2 次 Accept 应返回 false（满 2 个后取消）")
	}
	if s.Accept(3) {
		t.Error("第 3 次 Accept 应返回 false（已取消）")
	}
	s.End() // 空实现，不应 panic

	if len(got) != 3 {
		t.Errorf("累计接收 %d 个元素, 期望 3", len(got))
	}

	// 编译期断言：sinkFunc 实现 Sink 接口
	var _ Sink[int] = s
}

func TestStreamStructEmbedding(t *testing.T) {
	sp := &testSplitterator[int]{
		baseSplitterator: baseSplitterator[int]{estSize: 2, chars: SpSized | SpOrdered},
		items:            []int{7, 8},
	}
	// 编译期断言：pipeline 被 Stream 嵌入组合（未导出字段由包内构造路径使用）
	s := &Stream[int]{pipeline[int]{
		source: sp,
		chars:  sp.Characteristics(),
	}}
	if s.chars != SpSized|SpOrdered {
		t.Errorf("Stream.chars = %d, 期望 %d", s.chars, SpSized|SpOrdered)
	}
	if s.source != sp {
		t.Error("Stream.source 应为构造时传入的源")
	}
	if s.consumed {
		t.Error("新建 Stream 不应处于 consumed 状态")
	}
}

func TestKVAndConstraints(t *testing.T) {
	kv := KV[string, int]{Key: "a", Value: 1}
	if kv.Key != "a" || kv.Value != 1 {
		t.Errorf("KV 字段异常: %+v", kv)
	}

	// 编译期断言：约束覆盖常用数值类型（约束接口仅可作类型参数使用）
	assertInteger(int(0))
	assertInteger(uint8(0))
	assertFloat(float64(0))
	assertNumber(int32(0))
	assertNumber(float32(0))
}

func assertInteger[I Integer](_ I) {}
func assertFloat[F Float](_ F)     {}
func assertNumber[N Number](_ N)   {}

func TestEvalCtxFail(t *testing.T) {
	ec := &evalCtx{}
	e1 := &testError{"first"}
	e2 := &testError{"second"}

	if ec.fail(e1) {
		t.Error("fail 应返回 false（请求短路）")
	}
	if ec.fail(e2) { // 第二次仅短路，不覆盖首错
		t.Error("fail 应恒返回 false")
	}
	if !errors.Is(ec.err, e1) {
		t.Errorf("evalCtx.err = %v, 期望保留首错 %v", ec.err, e1)
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
