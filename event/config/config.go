package config

import (
    "os"
    "path/filepath"
    "strings"

    "gopkg.in/yaml.v3"
)

type ServerConfig struct {
    Port      int    `yaml:"port"`
    StaticDir string `yaml:"staticDir"`
    Env       string `yaml:"env"`
}

type KafkaConfig struct {
    Brokers []string `yaml:"brokers"`
}

type StarRocksConfig struct {
    FEHost   string `yaml:"feHost"`
    FEPort   int    `yaml:"fePort"`
    User     string `yaml:"user"`
    Password string `yaml:"password"`
    Database string `yaml:"database"`
}

type Config struct {
    Server     ServerConfig    `yaml:"server"`
    Kafka      KafkaConfig     `yaml:"kafka"`
    StarRocks  StarRocksConfig `yaml:"starrocks"`
}

func defaultConfig() Config {
    return Config{
        Server: ServerConfig{Port: 8088, StaticDir: "ui", Env: "dev"},
        Kafka:  KafkaConfig{Brokers: []string{"kafka:9092"}},
        StarRocks: StarRocksConfig{FEHost: "starrocks-fe", FEPort: 9030, User: "root", Password: "", Database: "eventdb"},
    }
}

func Load() Config {
    cfg := defaultConfig()
    path := os.Getenv("CONFIG_PATH")
    if strings.TrimSpace(path) == "" {
        // 默认使用仓库下 config/dev.yaml
        path = filepath.Join("config", "dev.yaml")
    }

    b, err := os.ReadFile(path)
    if err != nil {
        return cfg
    }
    var fileCfg Config
    if err := yaml.Unmarshal(b, &fileCfg); err != nil {
        return cfg
    }
    // 覆盖默认
    if fileCfg.Server.Port != 0 { cfg.Server.Port = fileCfg.Server.Port }
    if fileCfg.Server.StaticDir != "" { cfg.Server.StaticDir = fileCfg.Server.StaticDir }
    if fileCfg.Server.Env != "" { cfg.Server.Env = fileCfg.Server.Env }
    if len(fileCfg.Kafka.Brokers) > 0 { cfg.Kafka = fileCfg.Kafka }
    if fileCfg.StarRocks.FEHost != "" { cfg.StarRocks = fileCfg.StarRocks }
    return cfg
}