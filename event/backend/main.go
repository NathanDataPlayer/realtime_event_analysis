package main

import (
    "fmt"
    "net/http"
    "os"
    "time"

    "event/config"
    "event/logs"
    "event/routers"
)

func main() {
    cfg := config.Load()
    logger := logs.NewLogger()
    defer logger.Sync()

    r := routers.NewRouter(cfg, logger)

    addr := fmt.Sprintf(":%d", cfg.Server.Port)
    logger.Sugar().Infow("server.start",
        "addr", fmt.Sprintf("http://localhost:%d", cfg.Server.Port),
        "env", cfg.Server.Env,
        "static", cfg.Server.StaticDir,
    )
    srv := &http.Server{
        Addr:              addr,
        Handler:           r,
        ReadTimeout:       10 * time.Second,
        WriteTimeout:      10 * time.Second,
        ReadHeaderTimeout: 5 * time.Second,
        IdleTimeout:       60 * time.Second,
    }

    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        logger.Sugar().Errorw("server.error", "err", err)
        os.Exit(1)
    }
}