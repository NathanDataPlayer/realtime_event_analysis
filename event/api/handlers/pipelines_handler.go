package handlers

import (
    "encoding/json"
    "net/http"

    "event/config"
    "go.uber.org/zap"
)

type PipelinesHandler struct {
    Cfg    config.Config
    Logger *zap.Logger
}

func NewPipelinesHandler(cfg config.Config, logger *zap.Logger) *PipelinesHandler {
    return &PipelinesHandler{Cfg: cfg, Logger: logger}
}

type PipelineItem struct {
    Name   string `json:"name"`
    Status string `json:"status"`
}

func (h *PipelinesHandler) List(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    // 初稿返回空列表，后续接入 services/pipeline_service
    _ = json.NewEncoder(w).Encode([]PipelineItem{})
}