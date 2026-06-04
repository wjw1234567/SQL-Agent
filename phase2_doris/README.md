# Phase 2: 引入 Doris 查询加速

## 学习目标

理解 Paimon + Doris 湖仓一体的两种集成方式：
- **Doris Paimon Catalog**: 零 ETL，Doris 直接查询 Paimon 数据
- **Flink Doris Connector**: Flink 双写，Doris 独立存储查询

## 新增组件

| 组件 | 版本 | 端口 | 说明 |
|---|---|---|---|
| Doris FE | 2.0.3 | 9030 (MySQL), 8030 (WebUI) | 查询前端 |
| Doris BE | 2.0.3 | 9050 | 数据节点 |

## 快速开始

### 1. 启动环境

```bash
./start-phase2.sh
```

### 2. 验证 Doris

```bash
mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW PROC '/backends';"
```

### 3. 注册 Paimon Catalog

在 Flink SQL Client 或 Doris MySQL Client 中执行:
```sql
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
);
```

### 4. 查询验证

```bash
mysql -h 127.0.0.1 -P 9030 -uroot -e "SWITCH TO paimon_catalog.demo; SELECT COUNT(*) FROM orders;"
```

## 学习路线

1. 先做 `07_register_paimon.sql` — 体验零 ETL 查询
2. 再做 `08_query_via_doris.sql` — 学习 Doris 查询语法
3. 最后 `09_flink_to_doris.sql` — 学习双写模式
4. 尝试运行 `dual_writer.py` 体验自动化双写

## 停止环境

```bash
docker compose -f docker-compose-phase2.yml down
```
