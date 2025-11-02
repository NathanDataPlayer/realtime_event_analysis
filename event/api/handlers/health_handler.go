package handlers

import (
    "encoding/json"
    "net/http"
    "time"

    "event/config"
    "go.uber.org/zap"
)

type HealthHandler struct {
    Cfg    config.Config
    Logger *zap.Logger
}

func NewHealthHandler(cfg config.Config, logger *zap.Logger) *HealthHandler {
    return &HealthHandler{Cfg: cfg, Logger: logger}
}

type healthResp struct {
    OK       bool   `json:"ok"`
    Env      string `json:"env"`
    Server   string `json:"server"`
    Time     string `json:"time"`
}

func (h *HealthHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    resp := healthResp{
        OK:     true,
        Env:    h.Cfg.Server.Env,
        Server: "sr-ingest-api",
        Time:   time.Now().Format(time.RFC3339),
    }
    _ = json.NewEncoder(w).Encode(resp)
}