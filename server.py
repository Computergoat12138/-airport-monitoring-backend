"""
主服务入口
==========

这是整个后端的总入口，负责:
  1. 连接 MQTT Broker，订阅传感器数据
  2. 连接 MySQL（存储报警记录）和 InfluxDB（存储历史数据）
  3. 处理每条传感器消息: 存储 → 报警检测 → WebSocket 推送到前端
  4. 启动 FastAPI 服务（HTTP + WebSocket）

启动方式:
  python server.py

架构说明（复试可讲）:
  ┌──────────┐    MQTT     ┌──────────────┐   WebSocket   ┌──────────┐
  │ sensor.py │ ─────────→ │  server.py   │ ────────────→ │ 前端大屏  │
  │ (模拟器)  │  Publish   │ (本文件)      │   broadcast   │          │
  └──────────┘            │              │               └──────────┘
                          │ ↓   ↓   ↓    │
                          │ M  In  W     │
                          │ y  fl  e     │
                          │ S  u  b      │
                          │ Q  x  s      │
                          │ L  D  o      │
                          │    B  c      │
                          │       k      │
                          │       e      │
                          │       t      │
                          └──────────────┘

  关键设计点:
  - MQTT 采用 loop_start() 在后台线程运行，不阻塞主线程的 FastAPI
  - on_message 回调通过 asyncio.run_coroutine_threadsafe() 安全投递到
    WebSocket 的事件循环，解决跨线程通信问题
  - 数据库连接失败不会导致整个服务崩溃，每个模块独立异常处理
"""

import json
import logging
import os
import time
from datetime import datetime

import paho.mqtt.client as mqtt

# ---- 导入自定义模块 ----
from websocket import manager, mqtt_to_frontend, SENSOR_REGISTRY
from api import app
from config import CONFIG

# ================================================================
# 日志配置
# ================================================================
# 同时输出到控制台和 server.log 文件，
# 方便开发调试和线上排查。

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("server.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("server")


# ================================================================
# 配置区
# ================================================================
# 所有连接参数（MQTT/MySQL/InfluxDB/服务）已迁移到 config.py，
# 从环境变量或 .env 文件读取，代码里不再硬编码密码。
# 详见 config.py 顶部说明。


# ================================================================
# MySQL 连接（带异常保护）
# ================================================================

def init_mysql():
    """
    初始化 MySQL 连接

    如果连接失败，打印警告但不退出——
    报警存储功能不可用，但其他功能正常运行。
    这种做法叫"优雅降级"（Graceful Degradation），面试可以提。
    """
    try:
        import pymysql
        db = pymysql.connect(
            host=CONFIG["mysql_host"],
            user=CONFIG["mysql_user"],
            password=CONFIG["mysql_password"],
            database=CONFIG["mysql_database"],
            charset="utf8",
        )
        cursor = db.cursor()
        logger.info("MySQL 连接成功")
        return db, cursor
    except ImportError:
        logger.warning("PyMySQL 未安装，报警存储功能不可用")
        return None, None
    except Exception as e:
        logger.warning(f"MySQL 连接失败，报警存储功能不可用: {e}")
        return None, None


db, cursor = init_mysql()


# ================================================================
# InfluxDB 连接（带异常保护）
# ================================================================

def init_influx():
    """
    初始化 InfluxDB 写入客户端

    InfluxDB 用于存储传感器时序数据，
    写入失败不影响 WebSocket 推送和报警检测。
    """
    try:
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS

        client = InfluxDBClient(
            url=CONFIG["influx_url"],
            token=CONFIG["influx_token"],
            org=CONFIG["influx_org"],
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logger.info("InfluxDB 连接成功")
        return client, write_api
    except ImportError:
        logger.warning("influxdb-client 未安装，历史数据存储功能不可用")
        return None, None
    except Exception as e:
        logger.warning(f"InfluxDB 连接失败，历史数据存储功能不可用: {e}")
        return None, None


influx_client, influx_write_api = init_influx()


# ================================================================
# 报警检测
# ================================================================

def check_alarm(data: dict):
    """
    检测传感器数据是否触发报警（v2 — 双阈值）

    查 MySQL alarm_rule 表，新表结构支持 warn_value 和 alarm_value 双阈值:
      - value >= alarm_value  →  alarm 级别（红色）
      - value >= warn_value   →  warn  级别（黄色）
      - 否则                   →  正常

    MySQL 不可用时回退到 SENSOR_REGISTRY 的静态阈值。

    返回:
      (是否报警, 报警类型描述, 报警等级)
      例如: (True, "温度超限", "alarm")
    """
    sensor_type = data.get("sensor_type", "")
    value = float(data.get("value", 0))

    # ---- 方式1: 查 MySQL 动态规则表（双阈值） ----
    if cursor is not None:
        try:
            # 新表结构: warn_value, alarm_value, alarm_level, alarm_type
            # 查询逻辑: 先看是否超过 alarm_value，再看 warn_value
            sql = """
                SELECT warn_value, alarm_value, alarm_level, alarm_type
                FROM alarm_rule
                WHERE sensor_type = %s
                ORDER BY alarm_value DESC
            """
            cursor.execute(sql, (sensor_type,))
            rules = cursor.fetchall()  # 取所有规则（alarm 行 + warn 行）

            if rules:
                # 从高到低判断：先看 alarm 级别，再看 warn 级别
                for row in rules:
                    warn_val, alarm_val, level, atype = row
                    if value >= float(alarm_val):
                        return True, atype, level
                # alarm 没触发，检查 warn
                for row in rules:
                    warn_val, alarm_val, level, atype = row
                    if level == "warn" and value >= float(warn_val):
                        return True, atype, "warn"
                return False, None, None

        except Exception as e:
            logger.warning(f"MySQL 规则查询失败，回退到静态阈值: {e}")

    # ---- 方式2: 静态阈值兜底 ----
    device_id = data.get("device_id", "")
    sensor = SENSOR_REGISTRY.get(device_id, {})
    if not sensor:
        return False, None, None

    alarm_t = sensor.get("alarm_threshold", float("inf"))
    warn_t = sensor.get("warn_threshold", float("inf"))

    if value >= alarm_t:
        return True, f"{sensor.get('name','')}数值超标", "alarm"
    elif value >= warn_t:
        return True, f"{sensor.get('name','')}数值偏高", "warn"

    return False, None, None


# ================================================================
# MQTT 回调函数
# ================================================================

def on_connect(client, userdata, flags, rc):
    """
    MQTT 连接成功后的回调

    rc = 0 表示连接成功，然后订阅主题。
    如果连接失败，打印错误码方便排查。
    """
    if rc == 0:
        logger.info(f"MQTT 已连接 → {CONFIG['mqtt_broker']}:{CONFIG['mqtt_port']}")
        client.subscribe(CONFIG["mqtt_topic"])
        logger.info(f"MQTT 已订阅主题: {CONFIG['mqtt_topic']}")
    else:
        logger.error(f"MQTT 连接失败，返回码 rc={rc}")
        # rc 含义: 1=协议版本错误 2=ClientID被拒 3=服务不可用 4=用户名密码错误 5=未授权


def on_message(client, userdata, msg):
    """
    收到 MQTT 消息时的回调 ★核心处理流程★

    这条消息来自 MQTT 网络线程（不是主线程），
    所以访问 WebSocket 时必须用 broadcast_sync()（线程安全）。

    处理流程:
      ① 解析 JSON
      ② 保存 InfluxDB（历史数据）
      ③ 检测报警 → 报警则保存 MySQL
      ④ 格式转换后通过 WebSocket 推送到前端大屏
    """
    try:
        # ---------- ① 解析 ----------
        raw = msg.payload.decode("utf-8")
        data = json.loads(raw)
        logger.info(f"MQTT 收到 [{msg.topic}]: device={data.get('device_id')} "
                    f"type={data.get('sensor_type')} value={data.get('value')}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON 解析失败，丢弃本条数据: {e}")
        return
    except UnicodeDecodeError as e:
        logger.error(f"编码错误，丢弃本条数据: {e}")
        return

    # ---- 统一异常保护 ----
    # 如果某条数据处理出错，只丢弃当前这条，不影响后续消息
    try:
        # ---------- ② 保存 InfluxDB（历史时序数据） ----------
        if influx_write_api is not None:
            try:
                from influxdb_client import Point
                point = (
                    Point("sensor_data")
                    .tag("device_id", data.get("device_id", ""))
                    .tag("sensor_type", data.get("sensor_type", ""))
                    .field("value", float(data.get("value", 0)))
                )
                influx_write_api.write(
                    bucket=CONFIG["influx_bucket"],
                    org=CONFIG["influx_org"],
                    record=point,
                )
                logger.debug(f"InfluxDB 写入成功: {data.get('device_id')}")
            except Exception as e:
                logger.error(f"InfluxDB 写入失败: {e}")

        # ---------- ③ 报警检测与 MySQL 存储 ----------
        alarm_flag, alarm_type, alarm_level = check_alarm(data)

        if alarm_flag and cursor is not None:
            try:
                alarm_sql = """
                    INSERT INTO alarm_record
                        (device_id, alarm_type, alarm_level, alarm_value, alarm_time, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                alarm_values = (
                    data.get("device_id", ""),
                    alarm_type,
                    alarm_level,
                    float(data.get("value", 0)),
                    data.get("time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    "未处理",
                )
                cursor.execute(alarm_sql, alarm_values)
                db.commit()
                logger.warning(f"!!! 报警 !!! {alarm_type} | "
                               f"device={data.get('device_id')} "
                               f"value={data.get('value')} "
                               f"level={alarm_level}")
            except Exception as e:
                logger.error(f"MySQL 报警存储失败: {e}")
                if db:
                    try:
                        db.rollback()
                    except Exception:
                        pass

        # ---------- ④ WebSocket 推送（格式转换 + 广播） ----------
        # 这是"格式适配"的关键一步：
        #   MQTT 格式 → mqtt_to_frontend() → 前端大屏格式 → 广播给所有客户端
        frontend_data = mqtt_to_frontend(data)
        manager.broadcast_sync(frontend_data)

    except Exception as e:
        logger.error(f"消息处理异常（已丢弃本条数据）: {e}", exc_info=True)


# ================================================================
# MQTT 客户端初始化
# ================================================================

def init_mqtt():
    """
    创建 MQTT 客户端并绑定回调

    返回的 client 使用 loop_start() 在后台线程运行，
    不会阻塞主线程的 FastAPI 服务。
    """
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(
        CONFIG["mqtt_username"],
        CONFIG["mqtt_password"],
    )
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(CONFIG["mqtt_broker"], CONFIG["mqtt_port"], 60)
    mqtt_client.loop_start()  # 后台线程，非阻塞 ← 和 loop_forever() 的区别在这里
    logger.info("MQTT 客户端已启动（后台线程模式）")
    return mqtt_client


# ================================================================
# 主入口
# ================================================================

if __name__ == "__main__":
    import uvicorn
    import asyncio

    print("=" * 60)
    print("  机场结构健康监测平台 - 后端服务")
    print("=" * 60)
    print()
    print(f"  MQTT Broker:  {CONFIG['mqtt_broker']}:{CONFIG['mqtt_port']}")
    print(f"  MySQL:        {CONFIG['mysql_host']} / {CONFIG['mysql_database']}")
    print(f"  InfluxDB:     {CONFIG['influx_url']} / {CONFIG['influx_bucket']}")
    print(f"  FastAPI:      http://{CONFIG['fastapi_host']}:{CONFIG['fastapi_port']}")
    print(f"  WebSocket:    ws://{CONFIG['fastapi_host']}:{CONFIG['fastapi_port']}/ws")
    print()
    print("  启动 sensor.py 开始模拟传感器数据")
    print("=" * 60)

    # 1. 启动 MQTT 客户端（后台线程）
    mqtt_client = init_mqtt()

    # 2. 获取当前事件循环并绑定到 WebSocket 管理器
    #    这样 MQTT 线程才能安全地向 asyncio 投递协程
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    manager.bind_loop(loop)

    # 3. 启动 FastAPI（主线程阻塞在这里）
    #    前端大屏访问: http://你的IP:8000
    #    WebSocket 地址: ws://你的IP:8000/ws
    #    API 文档: http://你的IP:8000/docs （FastAPI 自动生成 Swagger）
    uvicorn.run(
        app,
        host=CONFIG["fastapi_host"],
        port=CONFIG["fastapi_port"],
        log_level="info",
    )
