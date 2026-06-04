# Paimon 核心参数完整参考

## 1. Catalog 参数

| 参数 | 必需 | 默认值 | 说明 |
|---|---|---|---|
| `type` | 是 | — | Catalog 类型: `filesystem` / `hive` / `jdbc` |
| `warehouse` | 是 | — | 数据仓库根路径 |
| `metastore` | 否 | — | Hive 方式时指定 metastore 类型 |

## 2. 表级核心参数

### 主键与分桶

| 参数 | 默认值 | 说明 |
|---|---|---|
| `bucket` | `1` | 每个分区的分桶数。影响写入并行度和读取并发。建议从 `2-4` 开始，后续按数据量调整。每个 bucket 对应一个文件。 |
| `bucket-key` | 主键字段 | 分桶键，决定数据如何分布到不同 bucket。 |

**Bucket 选择建议:**
- 小表 (<100 万行): `bucket=2`
- 中表 (<1 亿行): `bucket=4-8`
- 大表 (>1 亿行): `bucket=16-64`

### 分区

| 参数 | 默认值 | 说明 |
|---|---|---|
| `partition` | 无 | 在 DDL 中用 `PARTITIONED BY (col)` 指定。常用日期分区。 |

### Merge Engine

| 参数  | 默认值 | 说明 |
|---|---|---|
| `merge-engine` | `deduplicate` | Merge 引擎类型 |
| | | `deduplicate` — 默认，相同主键保留最后一条写入 |
| | | `partial-update` — 相同主键下逐字段更新 |
| | | `aggregation` — 相同主键下按聚合函数合并 |

**merge-engine 选择场景:**
- 普通 CDC: `deduplicate`
- 宽表逐字段填充: `partial-update`
- 实时指标聚合（SUM/COUNT/MIN/MAX）: `aggregation`

### Changelog Producer

| 参数 | 默认值 | 说明 |
|---|---|---|
| `changelog-producer` | `none` | Changelog 生成方式 |
| | | `none` — 不额外生成 changelog，需要时从文件计算 |
| | | `input` — 根据输入流的变更记录生成（推荐） |
| | | `lookup` — 通过全量读取对比生成，消耗较大 |

## 3. 查询参数 (通过 `/*+ OPTIONS() */` 设置)

| 参数 | 默认值 | 说明 |
|---|---|---|
| `scan.mode` | `latest` | 扫描模式: `latest` / `from-timestamp` / `from-snapshot` / `full` |
| `scan.snapshot-id` | — | 配合 `from-snapshot` 使用 |
| `scan.timestamp-millis` | — | 配合 `from-timestamp` 使用 |

## 4. Flink Session 参数

| 参数 | 推荐值 | 说明 |
|---|---|---|
| `execution.checkpointing.interval` | `30s` | Checkpoint 间隔，Paimon 提交依赖此设置 |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | 保证 Paimon 写入一致性 |
| `execution.runtime-mode` | `streaming` / `batch` | 流式或批式运行模式 |

## 5. 常见问题

**Q: Bucket 数可以修改吗？**
可以。ALTER TABLE 修改 bucket 后，新写入的数据会按新 bucket 数分布。旧数据保持不变。

**Q: merge-engine 建表后能改吗？**
不能。merge-engine 决定了底层文件的合并策略，建表后不可修改。

**Q: Paimon 和 Hudi/Iceberg 有什么区别？**
Paimon 的 Merge Engine 设计使其在流式写入场景更灵活（partial-update / aggregation），而 Iceberg 更侧重 ACID 和快照隔离。
