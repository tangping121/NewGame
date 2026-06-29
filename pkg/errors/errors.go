// Package errors 定义 API 返回的稳定业务错误码与 Error 类型。
package errors

import "fmt"

// Code 业务错误码，与 JSON 响应字段 code 对应。
type Code int

const (
	CodeOK           Code = 0    // 成功
	CodeInvalidParam Code = 1001 // 参数无效（缺字段、格式错）
	CodeUnauthorized Code = 1002 // 未授权（密码错、token 无效）
	CodeNotFound     Code = 1003 // 资源不存在
	CodeInternal     Code = 5000 // 服务端内部错误
)

// Error 带业务码的错误，实现 error 接口。
type Error struct {
	Code    Code   // 业务错误码
	Message string // 可读说明，可返回给客户端
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// New 构造业务错误。
//
// 参数:
//   - code: 错误码
//   - msg: 错误描述
func New(code Code, msg string) *Error {
	return &Error{Code: code, Message: msg}
}
