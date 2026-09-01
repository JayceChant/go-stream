package collector

// collector_test.go：子包收集器独立单测（不依赖根包，验证零依赖叶子包性质）。

import (
	"reflect"
	"strings"
	"testing"
)

func TestToSliceAndToSet(t *testing.T) {
	got := ToSlice[int]()
	a := got.Supplier()
	for _, v := range []int{1, 2, 3} {
		got.Accumulator(a, v)
	}
	if out := got.Finisher(a); len(out) != 3 || out[2] != 3 {
		t.Fatalf("ToSlice = %v", out)
	}
	// Combiner 拼接（并行合并语义）
	a1, a2 := ToSlice[int]().Supplier(), ToSlice[int]().Supplier()
	ToSlice[int]().Accumulator(a1, 1)
	ToSlice[int]().Accumulator(a2, 2)
	if merged := ToSlice[int]().Combiner(a1, a2); len(*merged) != 2 {
		t.Fatalf("Combiner = %v", *merged)
	}

	set := ToSet[int]()
	sa := set.Supplier()
	for _, v := range []int{1, 2, 2, 3} {
		set.Accumulator(sa, v)
	}
	if m := set.Finisher(sa); len(m) != 3 {
		t.Fatalf("ToSet = %v", m)
	}
}

func TestToMapLastWinsAndMerge(t *testing.T) {
	c := ToMap(func(s string) rune { return []rune(s)[0] }, func(s string) int { return len(s) })
	a := c.Supplier()
	for _, s := range []string{"a", "b", "a2"} {
		c.Accumulator(a, s)
	}
	if m := c.Finisher(a); m['a'] != 2 {
		t.Fatalf("last-wins: m['a'] = %d", m['a'])
	}

	cm := ToMapMerge(
		func(s string) rune { return []rune(s)[0] },
		func(s string) int { return len(s) },
		func(oldV, newV int) int { return oldV + newV },
	)
	ma := cm.Supplier()
	for _, s := range []string{"a", "b", "a2"} {
		cm.Accumulator(ma, s)
	}
	if m := cm.Finisher(ma); m['a'] != 3 {
		t.Fatalf("merge: m['a'] = %d", m['a'])
	}
}

func TestGroupingByOrdering(t *testing.T) {
	type kv struct {
		k string
		v int
	}
	c := GroupingBy(
		func(x kv) string { return x.k },
		func(x kv) int { return x.v },
	)
	a := c.Supplier()
	for _, x := range []kv{{"a", 1}, {"b", 2}, {"a", 3}} {
		c.Accumulator(a, x)
	}
	g := c.Finisher(a)
	if !reflect.DeepEqual(g["a"], []int{1, 3}) {
		t.Fatalf("GroupingBy = %v", g)
	}
}

func TestJoiningCountingReducingMapping(t *testing.T) {
	j := Joining(func(v int) string { return strings.Repeat("x", v) }, "-")
	ja := j.Supplier()
	for _, v := range []int{1, 2, 3} {
		j.Accumulator(ja, v)
	}
	if got := j.Finisher(ja); got != "x-xx-xxx" {
		t.Fatalf("Joining = %q", got)
	}

	cn := Counting[int]()
	ca := cn.Supplier()
	cn.Accumulator(ca, 1)
	cn.Accumulator(ca, 2)
	if n := cn.Finisher(ca); n != 2 {
		t.Fatalf("Counting = %d", n)
	}

	r := Reducing(0, func(a, b int) int { return a + b })
	ra := r.Supplier()
	for _, v := range []int{1, 2, 3, 4} {
		r.Accumulator(ra, v)
	}
	if got := r.Finisher(ra); got != 10 {
		t.Fatalf("Reducing = %d", got)
	}

	// Mapping 组合子：平方后再分组
	m := Mapping(
		func(v int) int { return v * v },
		ToSlice[int](),
	)
	ma := m.Supplier()
	for _, v := range []int{1, 2, 3} {
		m.Accumulator(ma, v)
	}
	if got := m.Finisher(ma); !reflect.DeepEqual(got, []int{1, 4, 9}) {
		t.Fatalf("Mapping = %v", got)
	}
}
