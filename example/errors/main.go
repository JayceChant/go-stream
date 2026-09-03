// Package main 演示错误即值模型：可失败源、Err 变体算子、Err() 查询与部分结果。
//
// 运行：go -C example run ./errors（example 为独立模块，不影响库的测试与覆盖率）
//
// 原则：可预期、可恢复的错误以 error 值传播——首错短路、已累积的
// 部分结果保留、Err() 返回首个错误（对齐 bufio.Scanner.Err 惯例）；
// 编程 bug（nil 回调、重复消费）则 panic，不做兜底。
package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/JayceChant/go-stream"
)

func main() {
	// ---------- 1. FromFunc：拉式可失败源 ----------
	// next 返回 (元素, 是否还有, 错误)；适配 IO/解析场景。
	// 出错时求值终止，ToSlice 保留出错前已产出的部分结果。
	n := 0
	rows := stream.FromFunc(func() (string, bool, error) {
		n++
		if n > 3 {
			return "", false, fmt.Errorf("第 %d 行读取失败", n)
		}
		return fmt.Sprintf("line-%d", n), true, nil
	})
	got := rows.ToSlice() // 部分结果：前 3 行
	fmt.Println("部分结果:", got)
	fmt.Println("Err():", rows.Err())

	// ---------- 2. MapErr：可失败的变换 ----------
	// 典型场景：解析字符串为数字，坏数据导致首错短路。
	s := stream.Of("10", "20", "abc", "40").
		MapErr(strconv.Atoi)
	nums := s.ToSlice() // [10 20]：解析到 abc 即停
	fmt.Println("MapErr 部分结果:", nums)
	fmt.Println("Err():", s.Err())

	// ---------- 3. FilterErr：可失败的谓词 ----------
	// 典型场景：校验规则需要查询外部状态（DB/缓存），查询可能失败。
	names := stream.Of("alice", "bob", "carol").
		FilterErr(func(name string) (bool, error) {
			if name == "bob" {
				return false, errors.New("用户状态查询失败")
			}
			return true, nil
		})
	fmt.Println("FilterErr 部分结果:", names.ToSlice()) // [alice]
	fmt.Println("Err():", names.Err())

	// ---------- 4. FlatMapErr：可失败的展开 ----------
	// 典型场景：逐条解析 CSV 行，坏行报错。
	csv := stream.Of("1,2,3", "bad", "4,5").
		FlatMapErr(func(line string) ([]int, error) {
			var out []int
			for f := range strings.SplitSeq(line, ",") {
				v, err := strconv.Atoi(f)
				if err != nil {
					return nil, fmt.Errorf("解析 %q 失败: %w", line, err)
				}
				out = append(out, v)
			}
			return out, nil
		})
	fmt.Println("FlatMapErr 部分结果:", csv.ToSlice()) // [1 2 3]
	fmt.Println("Err():", csv.Err())

	// ---------- 5. PeekErr：可失败的副作用（审计/日志） ----------
	audited := stream.Of(1, 2, 3).
		PeekErr(func(v int) error {
			if v == 2 {
				return errors.New("审计服务不可用")
			}
			return nil
		})
	fmt.Println("PeekErr 部分结果:", audited.ToSlice()) // [1]
	fmt.Println("Err():", audited.Err())

	// ---------- 6. 错误在管道中短路 ----------
	// MapErr 出错后，下游算子不再收到元素；管道各环节均正常收尾。
	s2 := stream.Of("1", "2", "x").
		MapErr(strconv.Atoi).                      // 首错：x 解析失败
		Filter(func(v int) bool { return v > 1 }). // 只收到出错前的 1、2
		Map(func(v int) int { return v * 100 })
	fmt.Println("多级管道部分结果:", s2.ToSlice()) // [200]
	fmt.Println("Err():", s2.Err())

	// ---------- 7. 编程 bug 走 panic（演示用 recover 捕获展示） ----------
	demoPanic()
}

// demoPanic 演示重复消费触发 panic：这是编程错误，应修复代码而非捕获。
func demoPanic() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("捕获到 panic（应修复代码而非捕获）:", r)
		}
	}()
	s := stream.Of(1, 2, 3)
	s.ToSlice() // 第一次消费：正常
	s.ToSlice() // 第二次消费：panic（流已被消费）
}
