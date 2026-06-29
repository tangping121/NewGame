// Package internalauth 为内部接口（/internal/*、Gate push、Game gRPC）提供共享密钥鉴权。
//
// 设计：密钥为空时全部放行（开发/单机），配置后强制校验，避免内网未授权
// 调用发奖、推送等敏感接口。HTTP 用请求头，gRPC 用 metadata。
package internalauth

import (
	"context"
	"crypto/subtle"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Header HTTP 鉴权头名称。
const Header = "X-Internal-Token"

// MetaKey gRPC metadata 键（必须小写）。
const MetaKey = "x-internal-token"

// equal 常量时间比较，避免时序侧信道。
func equal(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// HTTPMiddleware 包裹 handler 校验请求头；secret 为空时直接放行。
func HTTPMiddleware(secret string, next http.HandlerFunc) http.HandlerFunc {
	if secret == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !equal(r.Header.Get(Header), secret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// SetHTTP 在请求上设置鉴权头；secret 为空时不设置。
func SetHTTP(req *http.Request, secret string) {
	if secret != "" {
		req.Header.Set(Header, secret)
	}
}

// UnaryServerInterceptor 校验 gRPC metadata；secret 为空时放行。
func UnaryServerInterceptor(secret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if secret == "" {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		var got string
		if vals := md.Get(MetaKey); len(vals) > 0 {
			got = vals[0]
		}
		if !equal(got, secret) {
			return nil, status.Error(codes.Unauthenticated, "invalid internal token")
		}
		return handler(ctx, req)
	}
}

// WithOutgoing 在 gRPC 调用 context 中附加鉴权 metadata；secret 为空时原样返回。
func WithOutgoing(ctx context.Context, secret string) context.Context {
	if secret == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, MetaKey, secret)
}
