"""
WebSocket 连接管理器
====================

职责:
  1. 管理所有前端 WebSocket 连接（多客户端同时在线）
  2. 数据格式适配：MQTT 原始格式 → 前端大屏期望格式
  3. 将传感器数据实时广播给所有在线客户端

设计说明（复试可讲）:
  - 采用"连接池"模式管理多个前端客户端，支持大屏 + 移动端同时监控
  - 格式转换与通信逻辑解耦，mqtt_to_frontend() 可单独测试
  - broadcast() 内部自动清理断开的连接，避免内存泄漏
  - 通过 asyncio.run_coroutine_threadsafe() 解决 MQTT 线程与
    asyncio 事件循环之间的线程安全问题
"""

import asyncio
import logging
import time
from datetime import datetime

from fastapi import WebSocket

logger = logging.getLogger("websocket")


# ================================================================
# 传感器注册表
# ================================================================
# 作用：
#   1. 格式转换时补充 unit（单位），MQTT 消息里没有这个字段
#   2. 提供报警阈值，后端据此判断 alarm_level
#   3. 和前端 SENSORS 数组（line ~989）一一对应，新增传感器时
#      两边要同步更新
# ================================================================

SENSOR_REGISTRY = {
    "001": {
        "name": "温度传感器-1",
        "type": "temperature",
        "unit": "°C",
        "alarm_threshold": 35.0,   # 超过此值 → alarm（红色）
        "warn_threshold": 30.0,    # 超过此值 → warn（黄色）
    },
    "002": {
        "name": "湿度传感器-1",
        "type": "humidity",
        "unit": "%RH",
        "alarm_threshold": 85.0,
        "warn_threshold": 75.0,
    },
    "003": {
        "name": "倾角传感器-1",
        "type": "angle",
        "unit": "°",
        "alarm_threshold": 7.0,
        "warn_threshold": 5.0,
    },
    "004": {
        "name": "位移传感器-1",
        "type": "displacement",
        "unit": "mm",
        "alarm_threshold": 12.0,
        "warn_threshold": 8.0,
    },
    # ★ 扩展传感器 — 后续在 device 表和 alarm_rule 表加入对应数据即可激活
    "005": {
        "name": "加速度传感器-1",
        "type": "acceleration",
        "unit": "m/s²",
        "alarm_threshold": 5.0,
        "warn_threshold": 2.0,
    },
    "006": {
        "name": "应力传感器-1",
        "type": "stress",
        "unit": "MPa",
        "alarm_threshold": 50.0,
        "warn_threshold": 30.0,
    },
}


# ================================================================
# 格式转换函数
# ================================================================
# 这是"格式适配"的核心——把 MQTT 收到的传感器原始数据，
# 转成前端大屏 WebSocket 能直接消费的格式。
#
# 转换内容：
#   device_id  → sensor_id    （字段重命名）
#   time 字符串 → timestamp 数字 （时间格式统一为 Unix 时间戳）
#   补充 unit                  （从 SENSOR_REGISTRY 查表）
#   补充 alarm_level           （根据阈值判断：normal / warn / alarm）
# ================================================================

def mqtt_to_frontend(mqtt_data: dict) -> dict:
    """
    将 MQTT 消息转换为前端 WebSocket 格式

    输入示例:
      {
        "device_id": "001",
        "sensor_type": "temperature",
        "value": 32.5,
        "time": "2026-08-07 12:00:00"
      }

    输出示例:
      {
        "sensor_id": "001",
        "type": "temperature",
        "value": 32.5,
        "unit": "°C",
        "timestamp": 1723000000.0,
        "alarm_level": "warn"
      }
    """
    # ---- 提取原始字段 ----
    device_id = mqtt_data.get("device_id", "")
    sensor_type = mqtt_data.get("sensor_type", "")
    raw_value = mqtt_data.get("value", 0)
    value = float(raw_value)

    # ---- 查注册表补充字段 ----
    info = SENSOR_REGISTRY.get(device_id, {})
    unit = info.get("unit", "")
    alarm_threshold = info.get("alarm_threshold", float("inf"))
    warn_threshold = info.get("warn_threshold", float("inf"))

    # ---- 判断报警等级 ----
    if value >= alarm_threshold:
        alarm_level = "alarm"
    elif value >= warn_threshold:
        alarm_level = "warn"
    else:
        alarm_level = "normal"

    # ---- 时间戳转换：字符串 → 数字 ----
    time_str = mqtt_data.get("time", "")
    if time_str:
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            timestamp = dt.timestamp()
        except ValueError:
            timestamp = time.time()
    else:
        timestamp = time.time()

    # ---- 组装前端格式 ----
    return {
        "sensor_id": device_id,
        "type": sensor_type,
        "value": value,
        "unit": unit,
        "timestamp": timestamp,
        "alarm_level": alarm_level,
    }


# ================================================================
# WebSocket 连接管理器
# ================================================================

class ConnectionManager:
    """
    WebSocket 连接池

    维护所有在线前端客户端的 WebSocket 连接，
    提供广播能力，将一条数据同时推送给所有大屏/终端。

    使用方式:
      from websocket import manager

      # api.py 中注册新连接
      await manager.connect(websocket)

      # server.py 的 MQTT 回调中广播
      manager.broadcast_sync(data)
    """

    def __init__(self):
        # 存放所有在线的 WebSocket 连接对象
        self.clients: list[WebSocket] = []
        # 保存 asyncio 事件循环引用，供 MQTT 线程安全调用
        self._loop: asyncio.AbstractEventLoop | None = None

    # ---------- 事件循环绑定 ----------

    def bind_loop(self, loop: asyncio.AbstractEventLoop):
        """
        绑定 asyncio 事件循环。
        在 FastAPI 启动时调用，之后 MQTT 线程可以通过
        broadcast_sync() 安全地向这个循环投递协程。
        """
        self._loop = loop

    # ---------- 连接管理 ----------

    async def connect(self, ws: WebSocket):
        """接受一个新的 WebSocket 连接"""
        await ws.accept()
        self.clients.append(ws)
        logger.info(f"WebSocket 客户端已连接 (在线: {len(self.clients)})")

    def disconnect(self, ws: WebSocket):
        """移除一个断开的 WebSocket 连接"""
        if ws in self.clients:
            self.clients.remove(ws)
            logger.info(f"WebSocket 客户端已断开 (在线: {len(self.clients)})")

    # ---------- 数据广播 ----------

    async def broadcast(self, data: dict):
        """
        向所有在线客户端广播一条 JSON 数据。
        自动清理已断开但未正确移除的"僵尸连接"。
        """
        dead: list[WebSocket] = []
        for ws in self.clients:
            try:
                await ws.send_json(data)
            except Exception:
                # 客户端可能已关闭但没发 disconnect 信号
                dead.append(ws)

        for ws in dead:
            self.clients.remove(ws)

    def broadcast_sync(self, data: dict):
        """
        线程安全版广播——专供 MQTT 回调使用。

        MQTT 的 on_message 回调运行在 MQTT 自己的网络线程中，
        不能直接调用 async 函数。必须通过 run_coroutine_threadsafe
        把协程"投递"到 FastAPI 的事件循环里执行。
        """
        if self._loop is None:
            logger.warning("事件循环未绑定，跳过广播")
            return
        future = asyncio.run_coroutine_threadsafe(
            self.broadcast(data), self._loop
        )
        # 不等待结果，避免阻塞 MQTT 线程
        # 如果广播出错，broadcast() 内部已有异常处理

    # ---------- 工具属性 ----------

    @property
    def online_count(self) -> int:
        """当前在线客户端数"""
        return len(self.clients)


# ================================================================
# 模块级单例
# ================================================================
# server.py 和 api.py 都 import 这个 manager，
# 保证整个进程只有一份 WebSocket 连接池。

manager = ConnectionManager()
