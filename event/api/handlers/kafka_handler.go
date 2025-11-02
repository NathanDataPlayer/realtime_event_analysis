package handlers

import (
    "encoding/json"
    "net/http"

    "event/config"
    "event/services"
    "go.uber.org/zap"
)

type KafkaHandler struct {
    Cfg    config.Config
    Logger *zap.Logger
    Admin  *services.KafkaAdmin
}

func NewKafkaHandler(cfg config.Config, logger *zap.Logger) *KafkaHandler {
    return &KafkaHandler{Cfg: cfg, Logger: logger, Admin: services.NewKafkaAdmin(cfg)}
}

type TopicInfo struct {
    Name       string `json:"name"`
    Partitions int    `json:"partitions"`
}

func (h *KafkaHandler) ListTopics(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    topics, err := h.Admin.ListTopics(r.Context())
    if err != nil {
        h.Logger.Sugar().Warnw("kafka.list_topics.failed", "err", err)
        // 失败时也返回 200 与空数组，避免影响前端渲染
        _ = json.NewEncoder(w).Encode([]TopicInfo{})
        return
    }
    _ = json.NewEncoder(w).Encode(topics)
}