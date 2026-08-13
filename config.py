"""
集中配置模块
============

从环境变量读取所有连接配置，避免在代码里硬编码密码。

读取优先级：环境变量 > .env 文件 > 代码默认值

- 本地开发：把真实密码写在项目根目录的 .env 文件（已被 .gitignore 排除，不会提交到 Git）
- 部署：由 Docker Compose 或服务器环境变量注入
- .env.example 是模板，展示了需要哪些配置项
"""

import os


def _load_dotenv(path=None):
    """极简 .env 加载器（不依赖第三方库）

    逐行读取 KEY=VALUE，写入环境变量。
    只设置"环境变量里还不存在的键"，所以系统环境变量优先级更高。
    """
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # 跳过空行和注释行
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        # 没有 .env 文件也没关系，使用默认值
        pass


_load_dotenv()


def _int(key, default):
    return int(os.getenv(key, default))


CONFIG = {
    # ---- MQTT ----
    "mqtt_broker": os.getenv("MQTT_HOST", "127.0.0.1"),
    "mqtt_port": _int("MQTT_PORT", 1883),
    # acg/lol 是本地演示 broker 的账号，非敏感，保留为默认值
    "mqtt_username": os.getenv("MQTT_USER", "acg"),
    "mqtt_password": os.getenv("MQTT_PASSWORD", "lol"),
    "mqtt_topic": os.getenv("MQTT_TOPIC", "airport/sensor"),

    # ---- MySQL ----
    "mysql_host": os.getenv("MYSQL_HOST", "localhost"),
    "mysql_port": _int("MYSQL_PORT", 3306),
    "mysql_user": os.getenv("MYSQL_USER", "root"),
    "mysql_password": os.getenv("MYSQL_PASSWORD", ""),
    "mysql_database": os.getenv("MYSQL_DATABASE", "airport_system"),

    # ---- InfluxDB ----
    "influx_url": os.getenv("INFLUX_URL", "http://localhost:8086"),
    "influx_token": os.getenv("INFLUX_TOKEN", ""),
    "influx_org": os.getenv("INFLUX_ORG", "monior"),
    "influx_bucket": os.getenv("INFLUX_BUCKET", "sensor_data"),

    # ---- 服务 ----
    "fastapi_host": os.getenv("FASTAPI_HOST", "0.0.0.0"),
    "fastapi_port": _int("FASTAPI_PORT", 8000),
}
