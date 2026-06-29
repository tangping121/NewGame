// Package log 按配置级别创建 zap 控制台日志器。
package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level string) *zap.Logger {
	lvl := zapcore.InfoLevel
	_ = lvl.UnmarshalText([]byte(level))
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger.With(zap.String("pid", os.Getenv("HOSTNAME")))
}
