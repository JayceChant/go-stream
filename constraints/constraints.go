// Package constraints 提供跨包复用的类型约束（零依赖叶子包）。
//
// 数值约束族独立成包，使任意子包（如 collector 的 Summing）与根包
// 共享同一套约束而不产生对根包的反向依赖；本包不依赖任何其它包。
package constraints

// Integer 约束全部有符号与无符号整数类型，供 Range 等构造使用。
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Float 约束全部浮点类型。
type Float interface {
	~float32 | ~float64
}

// Number 约束全部数值类型（整数与浮点），供 Sum/Avg/Summing 等聚合使用。
type Number interface {
	Integer | Float
}
