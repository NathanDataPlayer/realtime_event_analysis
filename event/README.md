# 实时数据分析平台（Kafka → StarRocks → Metabase）

本项目提供 Docker Compose 与初始化脚本，打通 Kafka → StarRocks Routine Load → Metabase 的实时链路。所有镜像均为可用稳定版本：
- Kafka/Zookeeper：`confluentinc/cp-kafka:latest`、`confluentinc/cp-zookeeper:latest`
- StarRocks：`starrocks/fe-ubuntu:3.2-latest`、`starrocks/be-ubuntu:3.2-latest`（Apple Silicon 需仿真 `linux/amd64`）
- Metabase：`metabase/metabase:v0.49.13`
- Kafka UI：`provectuslabs/kafka-ui:latest`

## 目录
- `docker-compose.yml`：所有服务的编排
- `starrocks/init/`：建库、建表、物化视图、Routine Load 与 BE 注册脚本
- `producer/`：Python 数据生产器，持续向 Kafka 写入模拟事件

## 参考启动步骤

```bash
docker compose up -d --build
```

启动后：
- Kafka UI: `http://localhost:8080/`
- Metabase: `http://localhost:3000/`
- StarRocks FE (HTTP): `http://localhost:8030/`
- StarRocks FE (MySQL 协议): `localhost:9030`

Metabase 连接 StarRocks：
- 类型：`MySQL`
- Host：`starrocks-fe`
- Port：`9030`
- Username：`root`
- Database：`eventdb`

常用可视化：
- 每分钟 PV：查询物化视图 `mv_pv_per_minute`
- 每分钟营收：查询物化视图 `mv_revenue_per_minute`