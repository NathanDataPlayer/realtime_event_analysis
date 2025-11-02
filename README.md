# realtime_event_analysis

## 项目背景

- 面向"Kafka → StarRocks → 可视化"的端到端实时数仓分析项目。
- 目标是快速搭建消息生产、实时摄取（Routine Load）、作业管理与观测的闭环。
- 提供易用的前端 UI 与后端 API，降低对 StarRocks Routine Load 的使用门槛。
- 支持本地一键编排（Docker Compose），同时具备灵活的开发预览（Go 后端本地运行）。

## 核心价值

- **一站式**：从数据生成、Kafka 管理、StarRocks 摄取到可视化查看，链路完整。
- **低成本试用**：容器化部署，快速拉起 Kafka、StarRocks、Kafka UI、Metabase。
- **作业管理安全**：支持"暂停 → 修改 → 自动恢复"的作业编辑流程，符合 ALTER ROUTINE LOAD 要求。
- **可观测性**：UI 中实时查看作业状态与进度，辅助调试与运维。

## 技术架构

### 技术栈

- **后端**：Go（API，提供 UI 与 REST 服务）
- **前端**：原生 HTML/CSS/JS（ui/）
- **数据与消息**：Kafka，Zookeeper
- **实时数仓**：StarRocks FE（HTTP 8030 / MySQL 协议 9030）、BE
- **可视化分析**：Metabase

## 部署与运行

### 拉起依赖服务（Kafka/StarRocks 等）

```bash
docker compose up -d  # 或根据 README 步骤启动
```

### 开发后端与预览 UI

```bash
make run  # 后端监听 http://localhost:8088/，同时服务前端静态页面
```

### 打开 UI

- 浏览器访问 http://localhost:8088/

### 快速验证 Kafka 与 StarRocks

- **Kafka UI**：http://localhost:8080/
- **Metabase**：http://localhost:3000/（连接 StarRocks FE 9030）

## 效果演示

### (1) 数据可配置化接入

![数据可配置化接入](https://github.com/user-attachments/assets/f5080994-10b2-485c-a0d7-712284f2d627)

### (2) 支持作业的创建、修改与停止等管理

![作业管理](https://github.com/user-attachments/assets/f469fdd4-7f35-4d84-857a-068c7af60d05)

### (3) 整体集群的作业可视化

<img width="1564" height="885" alt="image" src="https://github.com/user-attachments/assets/f8c94991-ad31-44e3-ba60-7925d4ba74fc" />

### 实时接入源端数据到SR后，对接Metabase，可以实时分析数据

#### 实时DAU分析

![实时DAU分析](https://github.com/user-attachments/assets/cb804f03-476e-40fa-9b9e-e78407fe5bf7)
