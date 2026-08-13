"""
REST API 与 WebSocket 端点
==========================

基于 FastAPI 框架，提供:
  1. /ws                        — WebSocket 实时数据推送（前端大屏直连）
  2. GET /api/health            — 健康检查
  3. GET /api/sensor/history    — 传感器历史数据（InfluxDB 时序查询）
  4. GET /api/sensor/latest     — 所有传感器最新值
  5. GET /api/alarm/list        — 报警记录列表（MySQL 查询）

设计说明（复试可讲）:
  - CORS 中间件允许前端跨域访问（开发时 HTML 用 file:// 协议打开也能调接口）
  - 历史数据查询支持时间范围参数，利用 InfluxDB 的列式存储高效扫描
  - RESTful 风格接口设计，资源路径清晰
"""

import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

# ---- 导入 WebSocket 管理器单例 ----
from websocket import manager
from config import CONFIG

logger = logging.getLogger("api")

# ================================================================
# 创建 FastAPI 应用
# ================================================================

app = FastAPI(
    title="机场结构健康监测平台 API",
    description="混凝土结构健康监测系统后端接口",
    version="1.0.0",
)

# ================================================================
# CORS 跨域配置
# ================================================================
# 前端 HTML 文件可能用 file:// 协议打开，也可能部署在不同端口，
# 这里允许所有来源访问，开发阶段最方便。

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # 生产环境应改为前端实际域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================================================================
# WebSocket 端点 — 前端大屏直连这个地址
# ================================================================
# 前端 wsConnect()（line ~2996）修改地址指向这里:
#   const ws = new WebSocket("ws://你的电脑IP:8000/ws");
#
# 每条推送的消息格式由 websocket.mqtt_to_frontend() 定义:
#   {"sensor_id":"001","type":"temperature","value":32.5,
#    "unit":"°C","timestamp":1723000000,"alarm_level":"normal"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    WebSocket 实时数据通道

    客户端连上后不需要发送任何东西，服务端会主动推送
    每一条来自 MQTT 的传感器数据。
    客户端可以发心跳包（任意文本），服务端忽略内容。
    """
    await manager.connect(ws)
    try:
        # 保持连接，接收客户端发来的消息（心跳等）
        # 收到就丢弃——实际数据推送是服务端主动 broadcast
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception as e:
        logger.warning(f"WebSocket 异常断开: {e}")
        manager.disconnect(ws)


# ================================================================
# REST API — 健康检查
# ================================================================

@app.get("/api/health")
def health_check():
    """
    健康检查接口

    前端或运维可以用这个接口判断后端是否存活。
    返回当前服务时间和在线客户端数。
    """
    from datetime import datetime
    return {
        "status": "ok",
        "time": datetime.now().isoformat(),
        "websocket_clients": manager.online_count,
    }


# ================================================================
# REST API — 传感器最新值
# ================================================================

@app.get("/api/sensor/latest")
def get_sensor_latest():
    """
    查询所有传感器最新一条数据

    从 InfluxDB 中查每个 sensor 最近 1 分钟内的最新记录。
    如果 InfluxDB 不可用，返回空列表并给出提示。
    """
    # 延迟导入——如果 InfluxDB 没装，不影响其他接口
    try:
        from influxdb_client import InfluxDBClient
    except ImportError:
        return {
            "data": [],
            "message": "InfluxDB 客户端未安装 (pip install influxdb-client)"
        }

    try:
        client = InfluxDBClient(
            url=CONFIG["influx_url"],
            token=CONFIG["influx_token"],
            org=CONFIG["influx_org"],
        )
        query_api = client.query_api()

        # Flux 查询: 从 sensor_data 桶中取最近 60 秒的数据
        query = """
        from(bucket: "sensor_data")
          |> range(start: -1m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> last()
        """

        result = query_api.query(query)

        data = []
        for table in result:
            for record in table.records:
                data.append({
                    "time": record.get_time().isoformat(),
                    "device_id": record.values.get("device_id", ""),
                    "sensor_type": record.values.get("sensor_type", ""),
                    "value": record.get_value(),
                })

        client.close()
        return {"data": data, "count": len(data)}

    except Exception as e:
        logger.error(f"InfluxDB 查询失败: {e}")
        return {"data": [], "message": f"查询失败: {str(e)}"}


# ================================================================
# REST API — 历史数据查询
# ================================================================

@app.get("/api/sensor/history")
def get_sensor_history(
    device_id: str = Query(..., description="设备编号，如 001"),
    minutes: int = Query(10, ge=1, le=1440, description="查询最近多少分钟，默认10，最大1440(24小时)"),
):
    """
    查询某个传感器的历史数据（用于前端 ECharts 趋势图）

    参数:
      device_id  — 设备编号
      minutes    — 查最近多少分钟的数据

    返回:
      时间-值的列表，按时间升序排列
    """
    try:
        from influxdb_client import InfluxDBClient
    except ImportError:
        return {"data": [], "message": "InfluxDB 客户端未安装"}

    try:
        client = InfluxDBClient(
            url=CONFIG["influx_url"],
            token=CONFIG["influx_token"],
            org=CONFIG["influx_org"],
        )
        query_api = client.query_api()

        # Flux 查询: 按 device_id 过滤，取指定时间范围
        query = f"""
        from(bucket: "sensor_data")
          |> range(start: -{minutes}m)
          |> filter(fn: (r) => r._measurement == "sensor_data")
          |> filter(fn: (r) => r.device_id == "{device_id}")
          |> sort(columns: ["_time"])
        """

        result = query_api.query(query)

        data = []
        for table in result:
            for record in table.records:
                data.append({
                    "time": record.get_time().isoformat(),
                    "value": record.get_value(),
                })

        client.close()
        return {
            "device_id": device_id,
            "minutes": minutes,
            "data": data,
            "count": len(data),
        }

    except Exception as e:
        logger.error(f"InfluxDB 历史查询失败: {e}")
        return {"device_id": device_id, "data": [], "message": str(e)}


# ================================================================
# REST API — 报警记录查询
# ================================================================

@app.get("/api/alarm/list")
def get_alarm_list(
    limit: int = Query(50, ge=1, le=500, description="返回条数"),
    level: str = Query("all", description="报警等级: all / warn / alarm"),
):
    """
    查询历史报警记录（从 MySQL alarm_record 表）

    参数:
      limit — 最多返回多少条
      level — 筛选等级，all 表示全部
    """
    try:
        import pymysql
    except ImportError:
        return {"alarms": [], "message": "PyMySQL 未安装"}

    try:
        db = pymysql.connect(
            host=CONFIG["mysql_host"],
            port=CONFIG["mysql_port"],
            user=CONFIG["mysql_user"],
            password=CONFIG["mysql_password"],
            database=CONFIG["mysql_database"],
            charset="utf8",
        )
        cursor = db.cursor()

        if level == "all":
            sql = """
                SELECT device_id, alarm_type, alarm_level, alarm_value, alarm_time, status
                FROM alarm_record
                ORDER BY alarm_time DESC
                LIMIT %s
            """
            cursor.execute(sql, (limit,))
        else:
            sql = """
                SELECT device_id, alarm_type, alarm_level, alarm_value, alarm_time, status
                FROM alarm_record
                WHERE alarm_level = %s
                ORDER BY alarm_time DESC
                LIMIT %s
            """
            cursor.execute(sql, (level, limit))

        rows = cursor.fetchall()
        alarms = []
        for row in rows:
            alarms.append({
                "device_id": row[0],
                "alarm_type": row[1],
                "alarm_level": row[2],
                "alarm_value": float(row[3]) if row[3] else 0,
                "alarm_time": str(row[4]) if row[4] else "",
                "status": row[5],
            })

        db.close()
        return {"alarms": alarms, "count": len(alarms)}

    except Exception as e:
        logger.error(f"MySQL 报警查询失败: {e}")
        return {"alarms": [], "message": str(e)}
