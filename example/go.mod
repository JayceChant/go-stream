module github.com/JayceChant/go-stream/example

go 1.27

require github.com/JayceChant/go-stream v0.0.0

// 本模块仅承载可运行示例（不随根模块发布、不计入测试覆盖率）：
// 通过 replace 直接引用仓库根目录的库源码，根模块无需先行发布。
replace github.com/JayceChant/go-stream => ../
