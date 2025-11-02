package logs

import (
    "net/http"
    "time"

    "go.uber.org/zap"
)

func RequestLogger(logger *zap.Logger) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            start := time.Now()
            next.ServeHTTP(w, r)
            logger.Sugar().Infow("http.access",
                "method", r.Method,
                "path", r.URL.Path,
                "duration_ms", time.Since(start).Milliseconds(),
            )
        })
    }
}