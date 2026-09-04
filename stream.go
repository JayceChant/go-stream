// Package stream 提供基于 Go 1.27 泛型方法的 Java Stream 风格流式处理库。
//
// 流（Stream）不是数据结构，而是从数据源（slice、channel、迭代器、生成器等）
// 到结果之间的惰性管道：中间操作仅追加管道阶段不触发遍历，终止操作触发一次
// 单遍求值并返回新容器或聚合值。典型用法：
//
//	stream.Of(1, 2, 3).Filter(func(v int) bool { return v > 1 }).Map(strconv.Itoa).ToSlice()
//
// 详细设计（架构、错误模型、API 详案）见项目 spec/spec.md。
package stream

import "github.com/JayceChant/go-stream/constraints"

// 数值约束本体定义于零依赖叶子包 constraints（供 collector 等子包复用）；
// 根包以类型别名保留公开形态，stream.Integer/Float/Number 用法不变。

// Integer 约束全部有符号与无符号整数类型，供 Range 等构造使用。
type Integer = constraints.Integer

// Float 约束全部浮点类型。
type Float = constraints.Float

// Number 约束全部数值类型（整数与浮点），供 Sum/Avg 等聚合使用。
type Number = constraints.Number

// KV 是键值对元素，供 FromMap（map 源）、Enumerate（索引配对）等场景使用。
type KV[K comparable, V any] struct {
	Key   K
	Value V
}

// Stream 是对外公开的流类型：一条惰性管道的句柄。
//
// 通过包级构造函数（Of/FromSlice/FromSeq/Range 等）创建；中间操作以泛型方法
// 追加阶段并返回新的 *Stream（如 Map[U]）；终止操作触发一次求值并消费本流。
// 同一实例仅可被链接或消费一次，重复使用将 panic（编程错误，不可恢复）。
type Stream[T any] struct {
	pipeline[T]
}
