// Package internaltls builds mutual-TLS credentials for service-to-service
// gRPC connections.
package internaltls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"newgame/pkg/config"

	"google.golang.org/grpc/credentials"
)

func certificate(cfg config.InternalTLS) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load internal TLS certificate: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read internal TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("internal TLS CA contains no valid certificate")
	}
	return cert, roots, nil
}

// ServerCredentials 构造服务端 gRPC 凭据。
// 服务端固定使用 TLS 1.3，并要求客户端提交由同一内部 CA 签发的证书，
// 从传输层阻止未受信任的进程访问内部 RPC。
func ServerCredentials(cfg config.InternalTLS) (credentials.TransportCredentials, error) {
	cert, roots, err := certificate(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}), nil
}

// ClientCredentials 构造客户端 gRPC 凭据。
// 客户端既提交自身证书完成双向认证，也通过 ServerName 校验目标服务身份，
// 避免只校验证书链却连接到错误的内部服务。
func ClientCredentials(cfg config.InternalTLS) (credentials.TransportCredentials, error) {
	cert, roots, err := certificate(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ServerName:   cfg.ServerName,
	}), nil
}
