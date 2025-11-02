package logs

import (
    "go.uber.org/zap"
)

func NewLogger() *zap.Logger {
    cfg := zap.NewProductionConfig()
    cfg.Encoding = "json"
    logger, _ := cfg.Build()
    return logger
}