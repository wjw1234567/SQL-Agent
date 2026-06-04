# SQL-Agent

Paimon + Doris 湖仓一体架构，本地 LLM + RAG 检索生成 SQL。

## 项目结构

```
├── phase1_paimon/       # Phase 1: Kafka + Flink → Paimon 学习教程
│   ├── sql/             # Flink SQL 教程（含参数详解）
│   ├── flink-job/       # PyFlink DataStream API 作业
│   └── ...
├── phase2_doris/        # Phase 2: 引入 Doris 查询加速
│   ├── sql/             # Doris 集成教程
│   ├── flink-job/       # 双写作业
│   └── ...
└── docs/                # 设计文档和计划
```

## 学习路线

1. **Phase 1**: 学习 Paimon 流批一体存储
   - Docker 部署 Kafka + Flink + Paimon
   - Flink SQL 实时/离线写入 Paimon
   - Paimon 核心参数详解

2. **Phase 2**: 引入 Doris 查询加速
   - Doris Paimon Catalog 零 ETL 查询
   - Flink Doris Connector 双写模式

3. **未来**: AI SQL-Agent
   - 基于 Doris MySQL 接口接入本地 LLM
   - RAG 检索表结构 → 自动生成 SQL

详情请参见各阶段目录中的 README。
