package routers

import (
    "net/http"

    "event/api/handlers"
    "event/config"
    "event/logs"
    "github.com/go-chi/chi/v5"
    "github.com/go-chi/chi/v5/middleware"
    "go.uber.org/zap"
)

func NewRouter(cfg config.Config, logger *zap.Logger) *chi.Mux {
    r := chi.NewRouter()
    r.Use(middleware.RequestID)
    r.Use(middleware.RealIP)
    r.Use(middleware.Recoverer)
    r.Use(logs.RequestLogger(logger))

    // /api 路由组
    health := handlers.NewHealthHandler(cfg, logger)
    pipelines := handlers.NewPipelinesHandler(cfg, logger)
    kafka := handlers.NewKafkaHandler(cfg, logger)
    sr := handlers.NewStarRocksHandler(cfg, logger)
    summary := handlers.NewSummaryHandler(cfg, logger)

    r.Route("/api", func(api chi.Router) {
        api.Get("/health", health.GetHealth)
        api.Get("/summary", summary.Get)
        api.Get("/pipelines", pipelines.List)
        api.Get("/kafka/topics", kafka.ListTopics)
        api.Get("/starrocks/jobs", sr.ListJobs)
        api.Get("/starrocks/jobs/{name}", sr.GetJob)
        api.Post("/starrocks/jobs", sr.CreateJob)
        api.Post("/starrocks/jobs/{name}/pause", sr.PauseJob)
        api.Post("/starrocks/jobs/{name}/resume", sr.ResumeJob)
        api.Post("/starrocks/jobs/{name}/stop", sr.StopJob)
        api.Put("/starrocks/jobs/{name}", sr.UpdateJobProperties)
    })

    // 静态资源（默认挂载到仓库 ui/）
    fs := http.FileServer(http.Dir(cfg.Server.StaticDir))
    r.Handle("/*", fs)

    return r
}