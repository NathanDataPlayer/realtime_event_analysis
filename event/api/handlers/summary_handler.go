package handlers

import (
    "encoding/json"
    "net/http"

    "event/config"
    "event/services"
    "go.uber.org/zap"
)

type SummaryHandler struct {
    Cfg    config.Config
    Logger *zap.Logger
    Admin  *services.KafkaAdmin
    SR     *services.StarRocksClient
}

func NewSummaryHandler(cfg config.Config, logger *zap.Logger) *SummaryHandler {
    return &SummaryHandler{Cfg: cfg, Logger: logger, Admin: services.NewKafkaAdmin(cfg), SR: services.NewStarRocksClient(cfg)}
}

type Summary struct {
    Pipelines PipelinesSummary `json:"pipelines"`
    Jobs      JobsSummary      `json:"jobs"`
    Kafka     KafkaSummary     `json:"kafka"`
    Throughput ThroughputInfo  `json:"throughput"`
    Errors    ErrorsInfo       `json:"errors"`
    Lag       LagInfo          `json:"lag"`
    Anomalies []AnomalyItem    `json:"anomalies"`
}

type PipelinesSummary struct {
    Total        int `json:"total"`
    Running      int `json:"running"`
    Paused       int `json:"paused"`
    NeedSchedule int `json:"need_schedule"`
}

type JobsSummary struct {
    Total   int `json:"total"`
    Running int `json:"running"`
    Paused  int `json:"paused"`
    Failed  int `json:"failed"`
}

type KafkaSummary struct {
    Topics          int `json:"topics"`
    Partitions      int `json:"partitions"`
    UnderReplicated int `json:"under_replicated"`
}

type ThroughputInfo struct {
    Current int   `json:"current"`     // rows/min 或 messages/min
}

type ErrorsInfo struct {
    Last10m int `json:"last_10m"`
}

type LagInfo struct {
    P95ms int `json:"p95_ms"`
}

type AnomalyItem struct {
    Name  string `json:"name"`
    Type  string `json:"type"` // pipeline/job/topic
    State string `json:"state"`
    Count int    `json:"count"` // 可选计数，如错误数
}

func (h *SummaryHandler) Get(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    // Kafka
    topics, err := h.Admin.ListTopics(r.Context())
    if err != nil {
        h.Logger.Sugar().Warnw("summary.kafka.failed", "err", err)
        topics = []services.TopicInfo{}
    }
    kafka := KafkaSummary{Topics: len(topics), UnderReplicated: 0}
    // 估算分区总数
    parts := 0
    for _, t := range topics { parts += t.Partitions }
    kafka.Partitions = parts

    // StarRocks Jobs
    jobs, err := h.SR.ListRoutineLoad(r.Context())
    if err != nil {
        h.Logger.Sugar().Warnw("summary.jobs.failed", "err", err)
        jobs = []services.RLJob{}
    }
    var jobsRunning, jobsPaused, jobsFailed int
    for _, j := range jobs {
        switch s := normalizeState(j.State); s {
        case "RUNNING":
            jobsRunning++
        case "PAUSED":
            jobsPaused++
        case "FAILED":
            jobsFailed++
        }
    }
    jobsSum := JobsSummary{Total: len(jobs), Running: jobsRunning, Paused: jobsPaused, Failed: jobsFailed}

    // Pipelines（暂未实现服务，返回0摘要）
    pipes := PipelinesSummary{Total: 0, Running: 0, Paused: 0, NeedSchedule: 0}

    // 指标：基于事件表的实时近似统计
    // 事件表集合（包含 event_time 列的表）
    eventTables, err := h.SR.ListEventTables(r.Context())
    if err != nil {
        h.Logger.Sugar().Warnw("summary.list_event_tables.failed", "err", err)
        eventTables = []string{}
    }
    // 每分钟吞吐：近 1 分钟 event_time 内的行数总和
    tpVal, err := h.SR.CountRowsLastMinutes(r.Context(), eventTables, 1)
    if err != nil { h.Logger.Sugar().Warnw("summary.throughput.failed", "err", err); tpVal = 0 }
    tp := ThroughputInfo{Current: tpVal}
    // 错误行（近10分钟）：errors 表的最近 10 分钟
    err10m, err := h.SR.CountErrorsLastMinutes(r.Context(), 10)
    if err != nil { h.Logger.Sugar().Warnw("summary.errors.failed", "err", err); err10m = 0 }
    errs := ErrorsInfo{Last10m: err10m}
    // 消费延迟：now - max(event_time)（跨所有事件表的最新）
    lagMs, err := h.SR.ComputeFreshnessLagMs(r.Context(), eventTables)
    if err != nil { h.Logger.Sugar().Warnw("summary.lag.failed", "err", err); lagMs = 0 }
    lag := LagInfo{P95ms: lagMs}

    // 异常 Top3：从 jobs 中选非 RUNNING 的前三
    anomalies := make([]AnomalyItem, 0, 3)
    for _, j := range jobs {
        s := normalizeState(j.State)
        if s != "RUNNING" {
            anomalies = append(anomalies, AnomalyItem{Name: j.Name, Type: "job", State: s, Count: 0})
            if len(anomalies) == 3 { break }
        }
    }

    resp := Summary{
        Pipelines: pipes,
        Jobs:      jobsSum,
        Kafka:     kafka,
        Throughput: tp,
        Errors:    errs,
        Lag:       lag,
        Anomalies: anomalies,
    }
    _ = json.NewEncoder(w).Encode(resp)
}

func normalizeState(s string) string {
    if s == "" { return "UNKNOWN" }
    up := s
    // 部分 SR 状态可能是小写或其他命名，这里统一
    switch up {
    case "RUNNING", "PAUSED", "FAILED", "NEED_SCHEDULE", "NEED_SCHEDULING":
        return up
    default:
        return up
    }
}