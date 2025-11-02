package services

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "regexp"
    "strconv"
    "sort"
    "strings"
    "time"

    "event/config"
    _ "github.com/go-sql-driver/mysql"
)

type StarRocksClient struct {
    cfg config.Config
}

func NewStarRocksClient(cfg config.Config) *StarRocksClient {
    return &StarRocksClient{cfg: cfg}
}

type RLJob struct {
    Name  string `json:"name"`
    State string `json:"state"`
    Table string `json:"table"`
    Processed int `json:"processed"`
    Errors    int `json:"errors"`
}

// RLDetails 描述 Routine Load 的详细配置
type RLDetails struct {
    Name      string            `json:"name"`
    State     string            `json:"state"`
    Table     string            `json:"table"`
    CreateSQL string            `json:"create_sql,omitempty"`
    Properties map[string]string `json:"properties,omitempty"`
    Kafka     map[string]string `json:"kafka,omitempty"`
    Other     map[string]string `json:"other,omitempty"`
}

func (c *StarRocksClient) dsn() string {
    // 连接到目标数据库，简化查询
    return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true",
        c.cfg.StarRocks.User,
        c.cfg.StarRocks.Password,
        c.cfg.StarRocks.FEHost,
        c.cfg.StarRocks.FEPort,
        c.cfg.StarRocks.Database,
    )
}

// ListEventTables 返回包含 event_time 列的所有表名
func (c *StarRocksClient) ListEventTables(ctx context.Context) ([]string, error) {
    db, err := sqlOpen(c.dsn())
    if err != nil { return nil, err }
    defer db.Close()
    q := "SELECT TABLE_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND COLUMN_NAME = 'event_time' GROUP BY TABLE_NAME"
    rows, err := db.QueryContext(ctx, q, c.cfg.StarRocks.Database)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []string
    for rows.Next() {
        var name string
        if err := rows.Scan(&name); err != nil { return nil, err }
        out = append(out, name)
    }
    if err := rows.Err(); err != nil { return nil, err }
    sort.Strings(out)
    return out, nil
}

// CountRowsLastMinutes 统计指定表集合在最近 minutes 分钟内的总行数
func (c *StarRocksClient) CountRowsLastMinutes(ctx context.Context, tables []string, minutes int) (int, error) {
    if minutes < 1 { minutes = 1 }
    db, err := sqlOpen(c.dsn())
    if err != nil { return 0, err }
    defer db.Close()
    total := 0
    for _, t := range tables {
        q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE event_time >= NOW() - INTERVAL %d MINUTE", t, minutes)
        var cnt int
        if err := db.QueryRowContext(ctx, q).Scan(&cnt); err != nil {
            // 忽略单表错误，继续其他表
            continue
        }
        total += cnt
    }
    return total, nil
}

// CountErrorsLastMinutes 统计 errors 表最近 minutes 分钟的错误行数
func (c *StarRocksClient) CountErrorsLastMinutes(ctx context.Context, minutes int) (int, error) {
    if minutes < 1 { minutes = 1 }
    db, err := sqlOpen(c.dsn())
    if err != nil { return 0, err }
    defer db.Close()
    q := fmt.Sprintf("SELECT COUNT(*) FROM errors WHERE event_time >= NOW() - INTERVAL %d MINUTE", minutes)
    var cnt int
    if err := db.QueryRowContext(ctx, q).Scan(&cnt); err != nil { return 0, nil }
    return cnt, nil
}

// ComputeFreshnessLagMs 计算当前数据新鲜度延迟：now - max(event_time)（跨所有表）
func (c *StarRocksClient) ComputeFreshnessLagMs(ctx context.Context, tables []string) (int, error) {
    db, err := sqlOpen(c.dsn())
    if err != nil { return 0, err }
    defer db.Close()
    latestSec := int64(0)
    for _, t := range tables {
        q := fmt.Sprintf("SELECT UNIX_TIMESTAMP(MAX(event_time)) FROM %s", t)
        var sec sql.NullInt64
        if err := db.QueryRowContext(ctx, q).Scan(&sec); err != nil { continue }
        if sec.Valid && sec.Int64 > latestSec { latestSec = sec.Int64 }
    }
    if latestSec == 0 { return 0, nil }
    nowSec := time.Now().Unix()
    lagMs := int((nowSec - latestSec) * 1000)
    if lagMs < 0 { lagMs = 0 }
    return lagMs, nil
}

func (c *StarRocksClient) ListRoutineLoad(ctx context.Context) ([]RLJob, error) {
    db, err := sql.Open("mysql", c.dsn())
    if err != nil {
        return nil, err
    }
    defer db.Close()

    rows, err := db.QueryContext(ctx, "SHOW ROUTINE LOAD FROM "+c.cfg.StarRocks.Database)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    cols, err := rows.Columns()
    if err != nil {
        return nil, err
    }
    raw := make([]sql.RawBytes, len(cols))
    scan := make([]interface{}, len(cols))
    for i := range raw { scan[i] = &raw[i] }

    var out []RLJob
    for rows.Next() {
        if err := rows.Scan(scan...); err != nil { return nil, err }
        var job RLJob
        for i, c := range cols {
            key := strings.TrimSpace(c)
            val := string(raw[i])
            switch key {
            case "Name":
                job.Name = val
            case "State":
                job.State = val
            case "TableName":
                job.Table = val
            case "Statistic", "STATISTIC":
                // 解析统计文本，提取 loaded/success 与 error 行数
                p, e := parseStatisticCounts(val)
                if p >= 0 { job.Processed = p }
                if e >= 0 { job.Errors = e }
            default:
                // 尝试识别可能存在的计数列（不同版本列名可能不同）
                up := strings.ToUpper(key)
                if strings.Contains(up, "SUCCESS") || strings.Contains(up, "LOADED") || strings.Contains(up, "PROCESSED") {
                    if iv, err := strconv.Atoi(strings.TrimSpace(val)); err == nil { job.Processed = iv }
                } else if strings.Contains(up, "ERROR") && strings.Contains(up, "ROW") {
                    if iv, err := strconv.Atoi(strings.TrimSpace(val)); err == nil { job.Errors = iv }
                }
            }
        }
        out = append(out, job)
    }
    if err := rows.Err(); err != nil { return nil, err }
    return out, nil
}

// parseProps 尝试解析属性字符串为键值对，兼容 JSON 或 key=value 形式
func parseProps(s string) map[string]string {
    out := map[string]string{}
    ss := strings.TrimSpace(s)
    if ss == "" { return out }
    // 优先尝试 JSON
    var jm map[string]any
    if (strings.HasPrefix(ss, "{") && strings.HasSuffix(ss, "}")) || strings.HasPrefix(ss, "[") {
        if err := json.Unmarshal([]byte(ss), &jm); err == nil {
            for k, v := range jm { out[k] = fmt.Sprint(v) }
            return out
        }
    }
    // 退化解析：按照逗号分隔的 key=value
    parts := strings.Split(ss, ",")
    for _, p := range parts {
        kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
        if len(kv) != 2 { continue }
        k := strings.TrimSpace(kv[0])
        v := strings.Trim(kv[1], " \"'")
        if k != "" { out[k] = v }
    }
    if len(out) > 0 { return out }
    // 兜底：尝试按空格拆分
    fields := strings.Fields(ss)
    for _, f := range fields {
        kv := strings.SplitN(f, "=", 2)
        if len(kv) != 2 { continue }
        out[strings.TrimSpace(kv[0])] = strings.Trim(kv[1], " \"'")
    }
    return out
}

// GetRoutineLoadDetails 返回指定作业的详细配置
func (c *StarRocksClient) GetRoutineLoadDetails(ctx context.Context, name string) (*RLDetails, error) {
    if strings.TrimSpace(name) == "" { return nil, fmt.Errorf("empty name") }
    db, err := sqlOpen(c.dsn())
    if err != nil { return nil, err }
    defer db.Close()

    rows, err := db.QueryContext(ctx, "SHOW ROUTINE LOAD FROM "+c.cfg.StarRocks.Database)
    if err != nil { return nil, err }
    defer rows.Close()
    cols, err := rows.Columns(); if err != nil { return nil, err }
    raw := make([]sql.RawBytes, len(cols))
    scan := make([]interface{}, len(cols))
    for i := range raw { scan[i] = &raw[i] }
    var detail RLDetails
    var found bool
    for rows.Next() {
        if err := rows.Scan(scan...); err != nil { return nil, err }
        kv := map[string]string{}
        for i, c := range cols {
            key := strings.TrimSpace(c)
            val := string(raw[i])
            kv[key] = val
            switch key {
            case "Name":
                if val == name { found = true; detail.Name = val }
            case "State":
                detail.State = val
            case "TableName":
                detail.Table = val
            case "JobProperties", "Properties":
                if detail.Properties == nil { detail.Properties = map[string]string{} }
                for k, v := range parseProps(val) { detail.Properties[k] = v }
            case "DataSourceProperties":
                if detail.Kafka == nil { detail.Kafka = map[string]string{} }
                for k, v := range parseProps(val) { detail.Kafka[k] = v }
            }
        }
        if found { break }
    }
    if !found { return nil, fmt.Errorf("routine load not found: %s", name) }

    // 进一步获取 CREATE 语句（部分版本支持）
    // 如果失败则忽略，仅返回其他字段
    func() {
        q := fmt.Sprintf("SHOW CREATE ROUTINE LOAD FOR %s", name)
        rows2, err2 := db.QueryContext(ctx, q)
        if err2 != nil { return }
        defer rows2.Close()
        cols2, errc := rows2.Columns(); if errc != nil { return }
        raw2 := make([]sql.RawBytes, len(cols2))
        scan2 := make([]interface{}, len(cols2))
        for i := range raw2 { scan2[i] = &raw2[i] }
        if rows2.Next() {
            if err := rows2.Scan(scan2...); err == nil {
                // 合并所有列文本为一个展示字符串
                var sb strings.Builder
                for i := range raw2 {
                    if i > 0 { sb.WriteString(" ") }
                    sb.WriteString(string(raw2[i]))
                }
                detail.CreateSQL = sb.String()
            }
        }
    }()

    return &detail, nil
}

// parseStatisticCounts 尝试从 Statistic 文本中提取处理行与错误行（不同版本格式可能不同）
func parseStatisticCounts(s string) (processed int, errors int) {
    if s == "" { return -1, -1 }
    processed, errors = -1, -1

    // 优先尝试解析 JSON（StarRocks 新版本返回 JSON 格式统计）
    if strings.HasPrefix(strings.TrimSpace(s), "{") {
        var m map[string]any
        if err := json.Unmarshal([]byte(s), &m); err == nil {
            // 支持大小写不同的键名
            // processed 优先取 loadedRows / successRows / processedRows，其次 totalRows
            for _, k := range []string{"loadedRows", "successRows", "processedRows", "LoadedRows", "SuccessRows", "ProcessedRows"} {
                if v, ok := m[k]; ok {
                    if iv := toInt(v); iv >= 0 { processed = iv; break }
                }
            }
            if processed < 0 {
                for _, k := range []string{"totalRows", "TotalRows"} {
                    if v, ok := m[k]; ok {
                        if iv := toInt(v); iv >= 0 { processed = iv; break }
                    }
                }
            }
            for _, k := range []string{"errorRows", "ErrorRows"} {
                if v, ok := m[k]; ok {
                    if iv := toInt(v); iv >= 0 { errors = iv; break }
                }
            }
            return processed, errors
        }
    }

    // 回退：解析文本格式，如 "loaded rows: 123, error rows: 4"
    patterns := []struct{ re *regexp.Regexp; setter func(int) }{
        {regexp.MustCompile(`(?i)loaded\s*rows\s*[:=]\s*(\d+)`), func(v int) { processed = v }},
        {regexp.MustCompile(`(?i)success\s*rows\s*[:=]\s*(\d+)`), func(v int) { processed = v }},
        {regexp.MustCompile(`(?i)processed\s*rows\s*[:=]\s*(\d+)`), func(v int) { processed = v }},
        {regexp.MustCompile(`(?i)error\s*rows\s*[:=]\s*(\d+)`), func(v int) { errors = v }},
        {regexp.MustCompile(`(?i)total\s*rows\s*[:=]\s*(\d+)`), func(v int) { if processed < 0 { processed = v } }},
    }
    for _, p := range patterns {
        m := p.re.FindStringSubmatch(s)
        if len(m) == 2 {
            if v, err := strconv.Atoi(m[1]); err == nil { p.setter(v) }
        }
    }
    return processed, errors
}

// toInt 尝试将 JSON 值转换为整型
func toInt(v any) int {
    switch x := v.(type) {
    case float64:
        return int(x)
    case int:
        return x
    case int64:
        return int(x)
    case string:
        if iv, err := strconv.Atoi(strings.TrimSpace(x)); err == nil { return iv }
    }
    return -1
}

// RLCreateRequest 用于创建 Routine Load 作业的必要参数
type RLCreateRequest struct {
    Name   string            `json:"name"`
    Table  string            `json:"table"`
    Kafka  KafkaSource       `json:"kafka"`
    Columns []string         `json:"columns"`
    Set    map[string]string `json:"set"`
    Properties map[string]string `json:"properties"`
}

type KafkaSource struct {
    BrokerList string `json:"broker_list"`
    Topic      string `json:"topic"`
    GroupID    string `json:"group_id"`
}

// CreateRoutineLoad 根据请求参数拼装 CREATE ROUTINE LOAD 并执行
func (c *StarRocksClient) CreateRoutineLoad(ctx context.Context, req RLCreateRequest) error {
    if req.Name == "" || req.Table == "" || req.Kafka.BrokerList == "" || req.Kafka.Topic == "" || req.Kafka.GroupID == "" {
        return fmt.Errorf("missing required fields")
    }
    // 允许的 PROPERTIES 白名单，防止注入与非法键
    allowedProps := map[string]bool{
        "desired_concurrent_number": true,
        "max_batch_interval": true,
        "max_batch_rows": true,
        "max_batch_size": true,
        "strict_mode": true,
        "format": true,
        "jsonpaths": true,
    }
    // 组装 COLUMNS。如果给了 SET，则优先将 SET 转换为内联列映射：event_time = expr
    columns := make([]string, 0, len(req.Columns)+len(req.Set))
    if len(req.Columns) == 0 {
        columns = append(columns, "*")
    } else {
        columns = append(columns, req.Columns...)
    }
    if len(req.Set) > 0 && len(req.Columns) > 0 {
        inline := make([]string, 0, len(req.Set))
        for k, v := range req.Set {
            inline = append(inline, fmt.Sprintf("%s = %s", k, v))
        }
        sort.Strings(inline)
        columns = append(columns, inline...)
    }
    cols := strings.Join(columns, ", ")
    // 不再使用 SET 子句，避免不同版本语法差异
    setClause := ""
    // 组装 PROPERTIES（仅白名单）
    props := make([]string, 0, len(req.Properties))
    for k, v := range req.Properties {
        if !allowedProps[k] { continue }
        // 规范化下限：max_batch_rows >= 200000
        if k == "max_batch_rows" {
            if iv, err := strconv.Atoi(v); err != nil || iv < 200000 {
                v = "200000"
            }
        }
        // 统一使用双引号包裹值
        vv := strings.ReplaceAll(v, "\"", "\\\"")
        props = append(props, fmt.Sprintf("\"%s\" = \"%s\"", k, vv))
    }
    sort.Strings(props)
    propClause := ""
    if len(props) > 0 {
        propClause = fmt.Sprintf("\nPROPERTIES (\n  %s\n)", strings.Join(props, ",\n  "))
    }

    // 组装 FROM KAFKA
    fromKafka := fmt.Sprintf("\nFROM KAFKA (\n  \"kafka_broker_list\" = \"%s\",\n  \"kafka_topic\" = \"%s\",\n  \"property.group.id\" = \"%s\"\n)",
        req.Kafka.BrokerList, req.Kafka.Topic, req.Kafka.GroupID,
    )

    // 完整 SQL
    sql := fmt.Sprintf("CREATE ROUTINE LOAD %s\nON %s\nCOLUMNS(%s)%s%s%s",
        req.Name,
        req.Table,
        cols,
        setClause,
        propClause,
        fromKafka,
    )

    db, err := sqlOpen(c.dsn())
    if err != nil { return err }
    defer db.Close()
    // 调试输出 SQL，便于排查语法错误
    fmt.Println("[sr.create_rl] SQL =>\n" + sql)
    if _, err := db.ExecContext(ctx, sql); err != nil { return err }
    return nil
}

// UpdateRoutineLoadProperties 通过 ALTER ROUTINE LOAD 更新作业属性（白名单）
func (c *StarRocksClient) UpdateRoutineLoadProperties(ctx context.Context, name string, props map[string]string) error {
    if strings.TrimSpace(name) == "" { return fmt.Errorf("empty name") }
    if len(props) == 0 { return fmt.Errorf("no properties to update") }
    // 允许更新的属性白名单（按用户需求）
    allowed := map[string]bool{
        "desired_concurrent_number": true,
        "max_error_number": true,
        "max_batch_interval": true,
        "max_batch_rows": true,
        "max_batch_size": true,
        "jsonpaths": true,
        "json_root": true,
        "strip_outer_array": true,
        "strict_mode": true,
        "timezone": true,
    }
    pairs := make([]string, 0, len(props))
    // 排序保证生成 SQL 可预期
    keys := make([]string, 0, len(props))
    for k := range props { keys = append(keys, k) }
    sort.Strings(keys)
    for _, k := range keys {
        v := strings.TrimSpace(props[k])
        if !allowed[k] { continue }
        // 统一转义单引号，避免语法错误
        v = strings.ReplaceAll(v, "'", "''")
        pairs = append(pairs, fmt.Sprintf("'%s' = '%s'", k, v))
    }
    if len(pairs) == 0 { return fmt.Errorf("no valid properties") }
    q := fmt.Sprintf("ALTER ROUTINE LOAD FOR %s PROPERTIES ( %s )", name, strings.Join(pairs, ", "))
    db, err := sqlOpen(c.dsn())
    if err != nil { return err }
    defer db.Close()
    _, err = db.ExecContext(ctx, q)
    return err
}

// sqlOpen 抽象 sql.Open 以便在本文件中使用 strings 包
func sqlOpen(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) }

// 控制操作：暂停/恢复/停止 Routine Load
func (c *StarRocksClient) PauseRoutineLoad(ctx context.Context, name string) error {
    if strings.TrimSpace(name) == "" { return fmt.Errorf("empty name") }
    db, err := sqlOpen(c.dsn())
    if err != nil { return err }
    defer db.Close()
    _, err = db.ExecContext(ctx, fmt.Sprintf("PAUSE ROUTINE LOAD FOR %s", name))
    return err
}

func (c *StarRocksClient) ResumeRoutineLoad(ctx context.Context, name string) error {
    if strings.TrimSpace(name) == "" { return fmt.Errorf("empty name") }
    db, err := sqlOpen(c.dsn())
    if err != nil { return err }
    defer db.Close()
    _, err = db.ExecContext(ctx, fmt.Sprintf("RESUME ROUTINE LOAD FOR %s", name))
    return err
}

func (c *StarRocksClient) StopRoutineLoad(ctx context.Context, name string) error {
    if strings.TrimSpace(name) == "" { return fmt.Errorf("empty name") }
    db, err := sqlOpen(c.dsn())
    if err != nil { return err }
    defer db.Close()
    _, err = db.ExecContext(ctx, fmt.Sprintf("STOP ROUTINE LOAD FOR %s", name))
    return err
}