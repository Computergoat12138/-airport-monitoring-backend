-- ================================================================
-- 机场结构健康监测平台 — 数据库初始化脚本
-- ================================================================
-- 使用方式: 复制到 MySQL 客户端或命令行执行
--   mysql -u root -p < database.sql
-- ================================================================

-- ---- 建库 ----
CREATE DATABASE IF NOT EXISTS airport_system
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_general_ci;

USE airport_system;


-- ================================================================
-- 1. 传感器类型表（sensor_type）
-- ================================================================
-- 作用: 抽象传感器类型，统一管理 unit（单位）等元数据
-- 扩展方式: 新增传感器类型时 INSERT 一条即可

CREATE TABLE IF NOT EXISTS sensor_type (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    type_name   VARCHAR(50)  NOT NULL UNIQUE COMMENT '类型标识，如 temperature',
    unit        VARCHAR(20)  NOT NULL COMMENT '单位，如 °C',
    description VARCHAR(200) COMMENT '中文描述'
) COMMENT '传感器类型字典表';

-- 插入基础类型 + 扩展类型（加速度、应力）
INSERT INTO sensor_type (type_name, unit, description) VALUES
    ('temperature',   '°C',    '温度传感器'),
    ('humidity',      '%RH',   '湿度传感器'),
    ('angle',         '°',     '倾角传感器'),
    ('displacement',  'mm',    '位移传感器'),
    ('acceleration',  'm/s²',  '加速度传感器'),   -- ★ 新增
    ('stress',        'MPa',   '应力传感器');      -- ★ 新增


-- ================================================================
-- 2. 设备表（device）
-- ================================================================
-- 作用: 存储每个传感器的基本信息
-- 和前端 SENSORS 数组 (line ~989) 一一对应

CREATE TABLE IF NOT EXISTS device (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    device_id   VARCHAR(50)  NOT NULL UNIQUE COMMENT '设备编号，如 001',
    device_name VARCHAR(100) NOT NULL COMMENT '设备名称',
    sensor_type VARCHAR(50)  NOT NULL COMMENT '传感器类型',
    location    VARCHAR(100) COMMENT '安装位置',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- 外键: sensor_type 必须在 sensor_type 表中存在
    FOREIGN KEY (sensor_type) REFERENCES sensor_type(type_name)
) COMMENT '设备信息表';

-- 插入现有设备
INSERT INTO device (device_id, device_name, sensor_type, location) VALUES
    ('001', '温度传感器-1', 'temperature',  '航站楼A区'),
    ('002', '湿度传感器-1', 'humidity',     '机房'),
    ('003', '倾角传感器-1', 'angle',        '跑道结构'),
    ('004', '位移传感器-1', 'displacement', '桥梁区域');

-- ★ 扩展设备（后续再加传感器时取消注释）
-- INSERT INTO device (device_id, device_name, sensor_type, location) VALUES
--     ('005', '加速度传感器-1', 'acceleration', '桥梁区域'),
--     ('006', '应力传感器-1',   'stress',       '跑道结构');


-- ================================================================
-- 3. 报警规则表（alarm_rule）★ 重点修改
-- ================================================================
-- v2 改进: 双阈值设计，区分"预警"和"报警"两级
--   - warn_value:  预警阈值 → alarm_level='warn'  (黄色)
--   - alarm_value: 报警阈值 → alarm_level='alarm' (红色)
--   - alarm_level 统一使用 warn / alarm（和前端对齐）

CREATE TABLE IF NOT EXISTS alarm_rule (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    sensor_type VARCHAR(50)  NOT NULL COMMENT '传感器类型',
    warn_value  FLOAT        COMMENT '预警阈值（黄色）',
    alarm_value FLOAT        NOT NULL COMMENT '报警阈值（红色）',
    alarm_level VARCHAR(20)  NOT NULL DEFAULT 'alarm' COMMENT '报警等级: warn / alarm',
    alarm_type  VARCHAR(50)  NOT NULL COMMENT '报警描述',

    -- 外键
    FOREIGN KEY (sensor_type) REFERENCES sensor_type(type_name)
) COMMENT '报警规则表（双阈值）';

-- 插入规则 —— 每个传感器两行（warn + alarm）
INSERT INTO alarm_rule (sensor_type, warn_value, alarm_value, alarm_level, alarm_type) VALUES
    -- 温度: 30°C 预警, 35°C 报警
    ('temperature',  30,  35,  'warn',  '温度偏高'),
    ('temperature',  35,  999, 'alarm', '温度超限'),

    -- 湿度: 75% 预警, 85% 报警
    ('humidity',     75,  85,  'warn',  '湿度偏高'),
    ('humidity',     85,  100, 'alarm', '湿度超限'),

    -- 倾角: 5° 预警, 7° 报警
    ('angle',        5,   7,   'warn',  '倾角偏大'),
    ('angle',        7,   90,  'alarm', '倾角超限'),

    -- 位移: 8mm 预警, 12mm 报警
    ('displacement', 8,   12,  'warn',  '位移偏大'),
    ('displacement', 12,  999, 'alarm', '位移超限'),

    -- ★ 扩展: 加速度
    ('acceleration', 2.0, 5.0, 'warn',  '加速度偏大'),
    ('acceleration', 5.0, 999, 'alarm', '加速度超限'),

    -- ★ 扩展: 应力
    ('stress',       30,  50,  'warn',  '应力偏高'),
    ('stress',       50,  999, 'alarm', '应力超限');


-- ================================================================
-- 4. 报警记录表（alarm_record）
-- ================================================================
-- 和你的原表基本一致，仅优化了 alarm_level 的约束

CREATE TABLE IF NOT EXISTS alarm_record (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    device_id   VARCHAR(50)  NOT NULL COMMENT '设备编号',
    alarm_type  VARCHAR(50)  NOT NULL COMMENT '报警类型描述',
    alarm_level VARCHAR(20)  NOT NULL COMMENT 'warn 或 alarm',
    alarm_value FLOAT        NOT NULL COMMENT '触发报警时的数值',
    alarm_time  DATETIME     NOT NULL COMMENT '报警时间',
    status      ENUM('未处理','处理中','已关闭') DEFAULT '未处理' COMMENT '处理状态',

    FOREIGN KEY (device_id) REFERENCES device(device_id)
) COMMENT '报警记录表';


-- ================================================================
-- 验证
-- ================================================================
-- 查看表结构
-- DESC device;
-- DESC alarm_rule;
-- DESC alarm_record;
-- DESC sensor_type;

-- 查看数据
-- SELECT * FROM device;
-- SELECT * FROM alarm_rule ORDER BY sensor_type, alarm_level;
