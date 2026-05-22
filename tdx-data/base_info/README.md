# 板块数据管理

## 概述

本模块实现了股票板块信息的管理和查询功能，包括：
- 板块基础信息存储（block_info表）
- 股票与板块的从属关系存储（stock_block表）
- 数据导入脚本
- 数据查询测试

## 数据库表结构

### 1. block_info - 板块信息表

存储板块的基础信息。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| block_code | TEXT | 板块代码（主键），如：880081 |
| block_name | TEXT | 板块名称，如：轮动趋势 |
| update_time | TEXT | 更新时间 |

### 2. stock_block - 股票板块关系表

存储股票与板块的从属关系，一个股票可以属于多个板块。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | INTEGER | 自增主键 |
| code | TEXT | 股票代码（纯数字），如：600666 |
| block_code | TEXT | 板块代码，如：880081 |
| update_time | TEXT | 更新时间 |

**索引：**
- idx_stock_block_code: 基于股票代码的索引
- idx_stock_block_block_code: 基于板块代码的索引

## 数据源文件

### 1. all_block_names.txt
- 位置：`goldminer/all_block_names.txt`
- 格式：`板块代码,板块名称`
- 示例：`880081,轮动趋势`
- 包含：604个板块

### 2. tdx_block_mapping.json
- 位置：`goldminer/tdx_block_mapping.json`
- 格式：JSON对象，key为板块名称，value为股票代码列表
- 示例：`{"光通信": ["sz.000063", "sh.600522", ...]}`
- 包含：268个板块的股票映射关系

## 使用指南

### 1. 初始化数据库表

```bash
cd backend/stock_sqlite
python database.py
```

这将创建所有必要的表，包括block_info和stock_block。

### 2. 导入板块数据

```bash
cd goldminer/base_info
python block_info_save_to_db.py
```

该脚本会：
1. 解析all_block_names.txt文件
2. 将板块信息保存到block_info表
3. 解析tdx_block_mapping.json文件
4. 将股票-板块关系保存到stock_block表

**注意：** 脚本会先清空旧数据再插入新数据，确保数据一致性。

### 3. 测试数据查询

```bash
cd goldminer/base_info
python test_block_data.py
```

测试内容包括：
- 查询板块总数和示例
- 查询股票的板块归属
- 查询板块包含的股票
- 多板块联合查询

## API接口

### 1. 获取板块列表

**接口：** `GET /get-block-list`

**返回：**
```json
[
  {"code": "880081", "name": "轮动趋势"},
  {"code": "880082", "name": "板块趋势"},
  ...
]
```

### 2. 筛选股票（支持板块过滤）

**接口：** `GET /filter-stocks`

**参数：**
- recent_days: 最近天数（默认50）
- max_gain: 最大涨幅百分比（默认30）
- price_ratio: 股价不低于近期高点百分比（默认80）
- block_codes: 板块代码列表，逗号分隔（可选）

**示例：**
```
http://127.0.0.1:8000/filter-stocks?recent_days=50&max_gain=30&price_ratio=80&block_codes=880670,880656,880672
```

**返回：**
```json
[
  {"code": "000063", "name": "中兴通讯"},
  {"code": "600522", "name": "中天科技"},
  ...
]
```

## 数据统计

当前数据情况（截至最后更新）：
- 板块总数：604个
- 股票-板块关系数：约67,961条
- 覆盖板块数：268个（有股票映射的板块）

## 常见问题

### Q: 为什么有些板块在导入时被跳过？
A: tdx_block_mapping.json中的板块名称可能与all_block_names.txt中的名称不完全一致，导致无法匹配到板块代码。这些板块会被记录并跳过。

### Q: 如何更新板块数据？
A: 重新运行`block_info_save_to_db.py`脚本即可，脚本会自动清空旧数据并导入最新数据。

### Q: 一个股票可以属于多个板块吗？
A: 是的，stock_block表的设计支持一个股票属于多个板块。通过UNIQUE(code, block_code)约束确保同一股票在同一板块中不会重复。

## 维护建议

1. **定期更新**：建议定期运行导入脚本，保持板块数据的时效性
2. **数据备份**：在重新导入前，建议备份数据库文件
3. **日志监控**：关注导入过程中的警告信息，了解哪些板块未能成功匹配
