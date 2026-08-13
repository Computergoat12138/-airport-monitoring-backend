"""
传感器数据模拟器
================

独立运行的脚本，用于在没有真实传感器硬件时模拟数据。

工作原理:
  1. 连接到 MQTT Broker
  2. 每隔几秒生成一组模拟传感器数据
  3. 发布到主题 "airport/sensor"
  4. server.py 订阅同一主题，接收后处理

模拟的传感器类型:
  - 温度传感器 (device_id=001): 20~40°C，偶尔超过报警阈值 35°C
  - 湿度传感器 (device_id=002): 40~90%RH
  - 倾角传感器 (device_id=003): 0~8°
  - 位移传感器 (device_id=004): 0~15mm

使用方法:
  1. 先启动 server.py（订阅并处理数据）
  2. 再启动 sensor.py（开始发布模拟数据）
  3. 打开前端 HTML 即可看到实时数据

数据格式（与前端适配后的字段对应）:
  {
    "device_id": "001",        # 设备编号，对应前端 sensor_id
    "sensor_type": "temperature",  # 传感器类型
    "value": 32.5,             # 采集值
    "time": "2026-08-07 12:00:00"  # 采集时间
  }
"""

import json
import time
import random
import datetime

import paho.mqtt.client as mqtt

from config import CONFIG


# ================================================================
# 配置
# ================================================================
# 从集中配置模块 config.py 读取（环境变量 / .env 文件），
# 不再在代码里硬编码连接信息。

MQTT_BROKER = CONFIG["mqtt_broker"]
MQTT_PORT = CONFIG["mqtt_port"]
MQTT_USERNAME = CONFIG["mqtt_username"]
MQTT_PASSWORD = CONFIG["mqtt_password"]
MQTT_TOPIC = CONFIG["mqtt_topic"]

# 模拟数据发送间隔（秒）
SEND_INTERVAL = 5      # 每轮之间的间隔
BETWEEN_SENSORS = 1    # 每个传感器之间的间隔


# ================================================================
# 工具函数
# ================================================================

def now_str():
    """返回当前时间的格式化字符串"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ================================================================
# 传感器模拟配置
# ================================================================
# 每个传感器的数值范围和波动规则:
#   normal_range — 正常波动范围
#   abnormal_chance — 有百分之多少的概率产生异常值（触发报警）
#   abnormal_range — 异常值的范围

SENSOR_CONFIGS = [
    {
        "device_id": "001",
        "sensor_type": "temperature",
        "normal_range": (20.0, 32.0),      # 正常温度范围
        "abnormal_chance": 0.15,             # 15% 概率产生异常
        "abnormal_range": (35.0, 40.0),      # 异常温度范围（超 35°C 报警）
        "decimals": 2,                        # 保留小数位数
    },
    {
        "device_id": "002",
        "sensor_type": "humidity",
        "normal_range": (40, 75),
        "abnormal_chance": 0.15,
        "abnormal_range": (85, 95),          # 超 85%RH 报警
        "decimals": 0,
    },
    {
        "device_id": "003",
        "sensor_type": "angle",
        "normal_range": (0.0, 5.5),
        "abnormal_chance": 0.15,
        "abnormal_range": (7.0, 9.0),        # 超 7° 报警
        "decimals": 2,
    },
    {
        "device_id": "004",
        "sensor_type": "displacement",
        "normal_range": (0.0, 9.0),
        "abnormal_chance": 0.15,
        "abnormal_range": (12.0, 18.0),      # 超 12mm 报警
        "decimals": 2,
    },
    # ★ 扩展: 加速度传感器
    {
        "device_id": "005",
        "sensor_type": "acceleration",
        "normal_range": (0.0, 1.5),
        "abnormal_chance": 0.15,
        "abnormal_range": (5.0, 8.0),        # 超 5.0 m/s² 报警
        "decimals": 2,
    },
    # ★ 扩展: 应力传感器
    {
        "device_id": "006",
        "sensor_type": "stress",
        "normal_range": (0.0, 25.0),
        "abnormal_chance": 0.15,
        "abnormal_range": (50.0, 70.0),      # 超 50 MPa 报警
        "decimals": 2,
    },
]


def generate_sensor_data(config: dict) -> dict:
    """
    根据配置生成一条传感器数据

    大部分时间在 normal_range 内波动，
    有小概率 (abnormal_chance) 产生异常值，用于测试报警功能。
    """
    if random.random() < config["abnormal_chance"]:
        # 异常值
        low, high = config["abnormal_range"]
        value = round(random.uniform(low, high), config["decimals"])
    else:
        # 正常值
        low, high = config["normal_range"]
        value = round(random.uniform(low, high), config["decimals"])

    return {
        "device_id": config["device_id"],
        "sensor_type": config["sensor_type"],
        "value": value,
        "time": now_str(),
    }


# ================================================================
# 主程序
# ================================================================

def main():
    # ---- 连接 MQTT ----
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    print("=" * 50)
    print("  传感器数据模拟器")
    print(f"  MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Topic: {MQTT_TOPIC}")
    print("  按 Ctrl+C 停止")
    print("=" * 50)

    try:
        while True:
            for config in SENSOR_CONFIGS:
                # 生成数据
                data = generate_sensor_data(config)
                message = json.dumps(data, ensure_ascii=False)

                # 发布到 MQTT
                client.publish(MQTT_TOPIC, message)
                print(f"[{now_str()}] 发送 → {data['device_id']} "
                      f"({data['sensor_type']}) = {data['value']}")

                time.sleep(BETWEEN_SENSORS)

            # 每轮之间的间隔
            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print("\n模拟器已停止")
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
