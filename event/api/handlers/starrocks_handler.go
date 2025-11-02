package handlers

import (
    "encoding/json"
    "net/http"
    "strconv"
    "strings"

    "event/config"
    "event/services"
    "github.com/go-chi/chi/v5"
    "go.uber.org/zap"
)

type StarRocksHandler struct {
    Cfg    config.Config
    Logger *zap.Logger
    Client *services.StarRocksClient
}

func NewStarRocksHandler(cfg config.Config, logger *zap.Logger) *StarRocksHandler {
    return &StarRocksHandler{Cfg: cfg, Logger: logger, Client: services.NewStarRocksClient(cfg)}
}

type RLJob struct {
    Name   string `json:"name"`
    State  string `json:"state"`
    Table  string `json:"table"`
}

func (h *StarRocksHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    jobs, err := h.Client.ListRoutineLoad(r.Context())
    if err != nil {
        h.Logger.Sugar().Warnw("starrocks.list_jobs.failed", "err", err)
        _ = json.NewEncoder(w).Encode([]services.RLJob{})
        return
    }
    // 可选状态筛选与分页
    q := r.URL.Query()
    state := strings.ToUpper(strings.TrimSpace(q.Get("state")))
    // 允许的状态：RUNNING/PAUSED/FAILED；其他情况视为全部
    if state == "RUNNING" || state == "PAUSED" || state == "FAILED" {
        filtered := make([]services.RLJob, 0, len(jobs))
        for _, j := range jobs {
            if strings.ToUpper(j.State) == state { filtered = append(filtered, j) }
        }
        jobs = filtered
    }
    // 分页参数
    page := 1
    pageSize := 12
    if v := strings.TrimSpace(q.Get("page")); v != "" {
        if n, err := strconv.Atoi(v); err == nil && n > 0 { page = n }
    }
    if v := strings.TrimSpace(q.Get("page_size")); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            if n < 1 { n = 1 }
            if n > 100 { n = 100 }
            pageSize = n
        }
    }
    total := len(jobs)
    start := (page - 1) * pageSize
    if start < 0 { start = 0 }
    end := start + pageSize
    if start >= total { start = total; end = total }
    if end > total { end = total }
    // 设置分页头，兼容旧前端：仍返回数组，但带总数
    w.Header().Set("X-Total-Count", strconv.Itoa(total))
    w.Header().Set("X-Page", strconv.Itoa(page))
    w.Header().Set("X-Page-Size", strconv.Itoa(pageSize))
    _ = json.NewEncoder(w).Encode(jobs[start:end])
}

// GetJob 返回指定 Routine Load 的详细配置
func (h *StarRocksHandler) GetJob(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    name := chi.URLParam(r, "name")
    if strings.TrimSpace(name) == "" {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "missing name"})
        return
    }
    d, err := h.Client.GetRoutineLoadDetails(r.Context(), name)
    if err != nil {
        h.Logger.Sugar().Warnw("starrocks.get_job.failed", "name", name, "err", err)
        w.WriteHeader(http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
        return
    }
    _ = json.NewEncoder(w).Encode(d)
}

// CreateJob 通过 CREATE ROUTINE LOAD 创建作业
func (h *StarRocksHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    var req services.RLCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
        return
    }
    // 简要校验
    if req.Name == "" || req.Table == "" || req.Kafka.BrokerList == "" || req.Kafka.Topic == "" || req.Kafka.GroupID == "" {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "missing required fields"})
        return
    }
    // 规范化名称（避免空格）
    req.Name = strings.TrimSpace(req.Name)
    req.Table = strings.TrimSpace(req.Table)

    if err := h.Client.CreateRoutineLoad(r.Context(), req); err != nil {
        h.Logger.Sugar().Warnw("starrocks.create_job.failed", "err", err)
        w.WriteHeader(http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
        return
    }
    _ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "name": req.Name})
}

// PauseJob 暂停 Routine Load
func (h *StarRocksHandler) PauseJob(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    name := chi.URLParam(r, "name")
    if name == "" { w.WriteHeader(http.StatusBadRequest); _ = json.NewEncoder(w).Encode(map[string]string{"error":"missing name"}); return }
    if err := h.Client.PauseRoutineLoad(r.Context(), name); err != nil {
        h.Logger.Sugar().Warnw("starrocks.pause_job.failed", "name", name, "err", err)
        w.WriteHeader(http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
        return
    }
    _ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

// ResumeJob 恢复 Routine Load
func (h *StarRocksHandler) ResumeJob(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    name := chi.URLParam(r, "name")
    if name == "" { w.WriteHeader(http.StatusBadRequest); _ = json.NewEncoder(w).Encode(map[string]string{"error":"missing name"}); return }
    if err := h.Client.ResumeRoutineLoad(r.Context(), name); err != nil {
        h.Logger.Sugar().Warnw("starrocks.resume_job.failed", "name", name, "err", err)
        w.WriteHeader(http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
        return
    }
    _ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

// StopJob 停止 Routine Load（永久）
func (h *StarRocksHandler) StopJob(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    name := chi.URLParam(r, "name")
    if name == "" { w.WriteHeader(http.StatusBadRequest); _ = json.NewEncoder(w).Encode(map[string]string{"error":"missing name"}); return }
    if err := h.Client.StopRoutineLoad(r.Context(), name); err != nil {
        h.Logger.Sugar().Warnw("starrocks.stop_job.failed", "name", name, "err", err)
        w.WriteHeader(http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
        return
    }
    _ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

// UpdateJobProperties 更新 Routine Load 的属性（仅白名单）
func (h *StarRocksHandler) UpdateJobProperties(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    name := chi.URLParam(r, "name")
    if strings.TrimSpace(name) == "" {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "missing name"})
        return
    }
    var req struct {
        Properties map[string]string `json:"properties"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
        return
    }
    if len(req.Properties) == 0 {
        w.WriteHeader(http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]string{"error": "no properties"})
        return
    }
    // 根据官方要求，仅允许在 PAUSED 状态下修改。若非 PAUSED，则先暂停。
    paused := false
    resumed := false
    // 查询当前状态
    d, derr := h.Client.GetRoutineLoadDetails(r.Context(), name)
    if derr == nil {
        st := strings.ToUpper(strings.TrimSpace(d.State))
        if st != "PAUSED" {
            if err := h.Client.PauseRoutineLoad(r.Context(), name); err == nil {
                paused = true
            } else {
                h.Logger.Sugar().Warnw("starrocks.update_job.pause_failed", "name", name, "err", err)
            }
        } else {
            paused = true
        }
        // 执行修改
        if err := h.Client.UpdateRoutineLoadProperties(r.Context(), name, req.Properties); err != nil {
            h.Logger.Sugar().Warnw("starrocks.update_job.failed", "name", name, "err", err)
            w.WriteHeader(http.StatusInternalServerError)
            _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
            return
        }
        // 如果原状态是 RUNNING，修改后自动恢复
        if st == "RUNNING" {
            if err := h.Client.ResumeRoutineLoad(r.Context(), name); err == nil {
                resumed = true
            } else {
                h.Logger.Sugar().Warnw("starrocks.update_job.resume_failed", "name", name, "err", err)
            }
        }
    } else {
        // 查询状态失败时，仍尝试修改（可能被后端拒绝）。
        if err := h.Client.UpdateRoutineLoadProperties(r.Context(), name, req.Properties); err != nil {
            h.Logger.Sugar().Warnw("starrocks.update_job.failed_no_state", "name", name, "err", err)
            w.WriteHeader(http.StatusInternalServerError)
            _ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
            return
        }
    }
    _ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "paused": paused, "resumed": resumed})
}