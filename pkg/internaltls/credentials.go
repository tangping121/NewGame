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
