package services

import (
    "context"
    "sort"

    "event/config"
    "github.com/segmentio/kafka-go"
)

type KafkaAdmin struct {
    addrs []string
}

func NewKafkaAdmin(cfg config.Config) *KafkaAdmin {
    return &KafkaAdmin{addrs: cfg.Kafka.Brokers}
}

type TopicInfo struct {
    Name       string `json:"name"`
    Partitions int    `json:"partitions"`
}

func (ka *KafkaAdmin) ListTopics(ctx context.Context) ([]TopicInfo, error) {
    // 依次尝试地址，使用 kafka-go 的单连接读取分区，避免 advertised address 问题
    var lastErr error
    for _, addr := range ka.addrs {
        conn, err := kafka.DialContext(ctx, "tcp", addr)
        if err != nil {
            lastErr = err
            continue
        }
        parts, err := conn.ReadPartitions()
        _ = conn.Close()
        if err != nil {
            lastErr = err
            continue
        }
        // 聚合主题与分区数
        m := map[string]int{}
        for _, p := range parts {
            m[p.Topic]++
        }
        topics := make([]TopicInfo, 0, len(m))
        for name, cnt := range m {
            topics = append(topics, TopicInfo{Name: name, Partitions: cnt})
        }
        sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
        return topics, nil
    }
    return nil, lastErr
}