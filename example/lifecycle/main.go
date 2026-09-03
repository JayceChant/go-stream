// Package main 演示生命周期与可重放：OnClose/Close 资源管理、Cache 可重放工厂。
//
// 运行：go -C example run ./lifecycle（example 为独立模块，不影响库的测试与覆盖率）
//
// 要点：
//   - OnClose(f) 注册清理回调：终止求值结束时自动触发一次（耗尽/短路/
//     错误路径均触发）；显式 Close 亦触发且幂等；回调出错记首错（Err() 可查）
//   - Cache(s) 把一次性流转为可重放工厂：上游只求值一次，
//     每次调用返回全新的独立流
package main

import (
	"errors"
	"fmt"

	"github.com/JayceChant/go-stream"
)

func main() {
	// ---------- 1. 求值结束自动释放 ----------
	ch := make(chan int, 4)
	for _, v := range []int{1, 2, 3, 4} {
		ch <- v
	}
	close(ch)

	s := stream.FromChannel(ch).OnClose(func() error {
		fmt.Println("已释放底层连接")
		return nil
	})
	fmt.Println("求值结果:", s.ToSlice()) // 求值结束自动触发 OnClose，无需手动 Close

	// ---------- 2. 短路路径同样触发 ----------
	ch2 := make(chan int, 4)
	for _, v := range []int{5, 6, 7, 8} {
		ch2 <- v
	}
	close(ch2)

	s2 := stream.FromChannel(ch2).
		OnClose(func() error {
			fmt.Println("短路路径也触发了释放")
			return nil
		}).
		Limit(2) // 短路：只消费 2 个即停
	fmt.Println("短路结果:", s2.ToSlice())

	// ---------- 3. 显式 Close + 幂等 ----------
	closes := 0
	s3 := stream.Of(1, 2, 3).OnClose(func() error {
		closes++
		fmt.Println("清理回调执行次数:", closes)
		return nil
	})
	_ = s3.Close() // 显式关闭（未求值也可关闭）
	_ = s3.Close() // 幂等：重复调用不重复触发

	// ---------- 4. 回调链：按注册序执行，出错记首错 ----------
	fail := errors.New("清理失败")
	s4 := stream.Of(1, 2).
		OnClose(func() error { fmt.Println("回调 1"); return nil }).
		OnClose(func() error { fmt.Println("回调 2"); return fail }).
		OnClose(func() error { fmt.Println("回调 3"); return nil })
	fmt.Println("求值:", s4.Count())
	fmt.Println("Err() 返回回调链首错:", s4.Err())

	// ---------- 5. Cache：可重放工厂 ----------
	evals := 0
	source := stream.Range(1, 6).Peek(func(n int) { evals++ })
	replay := stream.Cache(source) // 此时尚未求值

	r1 := replay().ToSlice() // 首次调用：求值上游一次并物化
	r2 := replay().ToSlice() // 再次调用：直接重放（上游不再求值）
	r3 := replay().Count()
	fmt.Println("三次重放:", r1, r2, r3)
	// 两次 ToSlice 共输出 10 个元素；若每次重放都重新求值，Peek 会被调 10 次
	fmt.Println("上游 Peek 实际处理元素次数:", evals, "（仅物化那一次的 5 个）")

	// ---------- 6. 物化期出错：此后每次重放都携带该错误 ----------
	bad := stream.FromFunc(func() (int, bool, error) {
		return 0, false, errors.New("上游读取失败")
	})
	replayErr := stream.Cache(bad)
	sBad := replayErr()     // 每次调用返回携带该错误的空流
	empty := sBad.ToSlice() // 终止操作得空结果
	fmt.Println("出错后重放结果:", empty, "Err():", sBad.Err())
}
