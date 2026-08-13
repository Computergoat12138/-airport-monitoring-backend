# 机场结构健康监测平台 — 后端系统

## 项目概述

本项目是一个基于物联网架构的**混凝土结构健康监测系统**后端，服务于机场跑道/航站楼等基础设施的实时监控大屏。系统通过 MQTT 协议接入多类型传感器（温度、湿度、倾角、位移），将数据实时推送到前端 3D 可视化大屏，同时完成历史数据存储与异常报警。

后端：独立完成从数据接入、存储、到接口服务的全链路开发。

**技术栈：** Python / FastAPI / MQTT / WebSocket / MySQL / InfluxDB

---

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                     传感器层                             │
│  温度传感器  湿度传感器  倾角传感器  位移传感器           │
│  (device=001) (device=002) (device=003) (device=004)    │
└────────────────────┬────────────────────────────────────┘
                     │  MQTT (Publish/Subscribe)
                     ↓
┌─────────────────────────────────────────────────────────┐
│                   消息中间件层                             │
│              MQTT Broker (mosquitto)                     │
│              Topic: airport/sensor                       │
└────────────────────┬────────────────────────────────────┘
                     │  Subscribe
                     ↓
┌─────────────────────────────────────────────────────────┐
│                 后端服务层 (server.py)                   │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ 数据解析与    │  │ 报警检测      │  │ 格式适配      │  │
│  │ 异常保护      │  │ (阈值+MySQL   │  │ mqtt_to_      │  │
│  │              │  │  alarm_rule)  │  │ frontend()    │  │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘  │
│         │                 │                   │          │
│         ↓                 ↓                   ↓          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ InfluxDB     │  │ MySQL         │  │ WebSocket     │  │
│  │ 时序历史数据  │  │ 报警业务记录   │  │ 实时推送      │  │
│  └──────────────┘  └──────────────┘  └───────┬───────┘  │
│                                               │          │
└───────────────────────────────────────────────┼──────────┘
                                                │
                     ┌──────────────────────────┘
                     │  WebSocket
                     ↓
┌─────────────────────────────────────────────────────────┐
│                   前端展示层                             │
│         3D 可视化大屏 (Three.js + ECharts)               │
│         混凝土结构模型 + 传感器实时标记 + 告警弹窗        │
└─────────────────────────────────────────────────────────┘
```

---

## 模块说明

| 文件 | 职责 | 关键设计 |
|------|------|----------|
| `server.py` | 主入口，MQTT 订阅 + 数据库 + FastAPI 启动 | MQTT 后台线程 + asyncio 事件循环的线程安全通信 |
| `sensor.py` | 传感器数据模拟器（独立运行） | 正常值/异常值概率混合，可配置阈值 |
| `api.py` | REST API + WebSocket 端点 | CORS 跨域、RESTful 设计、异常降级 |
| `websocket.py` | WebSocket 连接池 + 格式适配 | 广播模式、僵尸连接自动清理、线程安全 |

### 数据流向

```
sensor.py ──MQTT──→ server.py (on_message)
                      │
                      ├──→ InfluxDB（历史数据，每条都存）
                      ├──→ MySQL（仅报警数据，触发阈值才存）
                      └──→ WebSocket 广播（实时推送给所有前端大屏）
```

---

## 格式适配说明

前后端数据格式的桥梁是 `websocket.py` 中的 `mqtt_to_frontend()` 函数：

```
MQTT 输入                          WebSocket 输出（前端消费）
─────────────────────────────      ─────────────────────────────
device_id: "001"          →        sensor_id: "001"
sensor_type: "temperature"→        type: "temperature"
value: 32.5                        value: 32.5
（无）                    ← 补充    unit: "°C"        (查注册表)
（无）                    ← 计算    alarm_level: "warn" (阈值判定)
time: "2026-08-07 12:00"  →        timestamp: 1723000000 (Unix时间戳)
```

---

## 快速启动

### 1. 环境准备

```bash
pip install paho-mqtt pymysql influxdb-client fastapi uvicorn
```

### 2. 启动 MQTT Broker

```bash
# Windows: 下载 mosquitto 并启动
mosquitto -v

# 或 Docker
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto
```

### 3. 启动后端服务

```bash
# 终端1: 启动主服务（MQTT订阅 + API + WebSocket）
python server.py

# 终端2: 启动传感器模拟器（发布模拟数据）
python sensor.py
```

### 4. 前端接入

打开前端 HTML 文件，修改 WebSocket 连接地址：
```javascript
// 前端 line ~2996 附近
const ws = new WebSocket("ws://localhost:8000/ws");
```

### 5. 查看 API 文档

浏览器打开 `http://localhost:8000/docs` — FastAPI 自动生成的 Swagger 交互式文档。

---

## 数据库表设计

### MySQL — alarm_record（报警记录表）

```sql
CREATE TABLE alarm_record (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    device_id   VARCHAR(10)  NOT NULL COMMENT '设备编号',
    alarm_type  VARCHAR(50)  NOT NULL COMMENT '报警类型',
    alarm_level VARCHAR(10)  NOT NULL COMMENT '报警等级: warn/alarm',
    alarm_value FLOAT        NOT NULL COMMENT '触发报警时的数值',
    alarm_time  DATETIME     NOT NULL COMMENT '报警时间',
    status      VARCHAR(20)  DEFAULT '未处理' COMMENT '处理状态'
);
```

### MySQL — alarm_rule（报警规则表）

```sql
CREATE TABLE alarm_rule (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    sensor_type VARCHAR(30) NOT NULL COMMENT '传感器类型',
    max_value   FLOAT       NOT NULL COMMENT '报警阈值',
    alarm_level VARCHAR(10) NOT NULL COMMENT 'warn 或 alarm',
    alarm_type  VARCHAR(50) NOT NULL COMMENT '报警描述'
);
```

### InfluxDB — sensor_data（时序数据）

```
Measurement: sensor_data
Tags:  device_id, sensor_type
Field: value (float)
```

---

### Q1: 简单介绍一下这个项目

> 这是一个机场混凝土结构健康监测平台的后端系统。我负责从传感器数据接入到接口服务的完整后端开发。系统通过 MQTT 协议接收温度、湿度、倾角、位移四类传感器数据，采用 InfluxDB 存储时序历史数据，MySQL 存储报警业务记录，通过 FastAPI 提供 REST API，并用 WebSocket 向前端 3D 可视化大屏实时推送数据。核心挑战在于数据格式适配、MQTT 线程与 asyncio 事件循环的线程安全问题，以及系统的模块化设计。

### Q2: 为什么选择 MQTT 而不是 HTTP？

> MQTT 是物联网场景的标准协议。相比 HTTP 的请求-响应模型，MQTT 的发布/订阅模式有三个优势：第一，传感器和服务器解耦，新增传感器类型不需要改服务器代码；第二，协议头部极小（最小仅 2 字节），适合大量传感器高频上报；第三，支持 QoS 分级，重要报警数据可以保证送达。HTTP 适合"拉"数据，MQTT 适合"推"数据。

### Q3: 为什么同时用 MySQL 和 InfluxDB？

> 这是典型的"异构存储"设计。InfluxDB 是时序数据库，针对时间范围扫描做了列式存储优化，适合存"温度每 5 秒一条"这类高频时序数据。MySQL 存的是报警这类有业务语义的结构化数据——报警需要关联设备信息、处理人、处理结果等，这是关系数据库的强项。两者各司其职，比单独用一个更高效。

### Q4: MQTT 回调里怎么安全地操作 WebSocket？

> 这是我遇到的一个核心技术问题。MQTT 的 `on_message` 回调运行在 Paho 库自己的网络线程中，而 WebSocket 的 `send_json()` 是 asyncio 协程，不能在非 asyncio 线程中直接 await。我的解决方案是：在主线程启动时把 asyncio 事件循环绑定到 WebSocket 管理器，MQTT 回调通过 `asyncio.run_coroutine_threadsafe()` 将广播协程安全投递到事件循环中执行。这样既保证了线程安全，又没有引入额外的锁或队列开销。

### Q5: 系统怎么保证可靠性？

> 主要体现在三个方面。第一，MQTT 消息处理的每一层都有独立的 try/except，某条数据出错只丢弃当前条，不影响后续消息；第二，数据库连接失败时系统"优雅降级"——比如 MySQL 挂了，报警检测会回退到代码中的静态阈值，WebSocket 推送和 InfluxDB 存储不受影响；第三，日志同时输出到控制台和文件，方便排查问题。

### Q6: 如果传感器数量从 4 个扩展到 400 个，系统需要怎么改？

> 首先，MQTT 的发布/订阅模型天然支持水平扩展——400 个传感器只是多发 400 条消息，Broker 和订阅端不需要改架构。其次，后端这边主要瓶颈在数据库写入，InfluxDB 本身支持批量写入，可以把 `write_api.write()` 改成定时批量 flush。WebSocket 广播层如果客户端太多，可以换成 Redis Pub/Sub 做多进程广播。最后，FastAPI 可以部署多个 worker 进程配合 Nginx 负载均衡。

---

## 目录结构

```
机场项目/
├── server.py          # 主服务入口（MQTT订阅 + 数据存储 + FastAPI启动）
├── sensor.py          # 传感器数据模拟器（独立运行）
├── api.py             # REST API 路由 + WebSocket 端点
├── websocket.py       # WebSocket 连接池 + 格式适配
├── data_queue.py      # [旧] 队列传递数据 → 已废弃，改用直接广播
├── websocket_server.py# [旧] 原始 WebSocket 实现 → 已重构为 websocket.py
├── README.md          # 项目说明（本文件）
└── server.log         # 运行日志（自动生成）
```

---

## 改进方向

1. **配置外置** — 将数据库密码等敏感信息迁移到 `.env` 文件
2. **批量写入** — InfluxDB 改为定时批量 flush，减少网络开销
3. **消息队列缓冲** — 在 MQTT 和数据库之间加一层内存缓冲，削峰填谷
4. **告警升级** — 加入钉钉/邮件通知，报警超过 N 分钟未处理自动升级
5. **数据回放** — 支持历史时间段的数据回放，用于事故复盘
6. **容器化部署** — 编写 Docker Compose 一键启动全套服务
