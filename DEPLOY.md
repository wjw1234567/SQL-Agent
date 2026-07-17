# SQL-Agent 全栈部署教程

## 同时部署 Phase 1 (Kafka+Flink+Paimon) + Phase 2 (Doris) + 本地 RAG

---

### 📋 整体架构概览

本教程将一个完整的湖仓一体 + AI 问答系统部署到你的电脑上，包含三个子系统：

```
┌─────────────────────────────────────────────────────────────────┐
│  子系统 1: 数据管道 (Docker)                                     │
│  Kafka → Flink → Paimon (湖存储)                                 │
│         ↕                                                       │
│  Doris (查询加速, MySQL 协议)                                     │
├─────────────────────────────────────────────────────────────────┤
│  子系统 2: 本地 RAG 问答 (Python + Ollama)                       │
│  文档 → BGE-M3(向量化) → ChromaDB(向量库)                        │
│  用户提问 → 检索相关文档 → Qwen3.5(回答) → Web UI                │
├─────────────────────────────────────────────────────────────────┤
│  子系统 3 (未来): LLM 通过 Doris MySQL 接口自动生成 SQL            │
└─────────────────────────────────────────────────────────────────┘
```

**系统要求**：Windows 11, 16GB+ 内存, 50GB+ 磁盘, RTX 5060 Ti 16GB (RAG 需要 GPU)

---

### 🔧 准备工作

#### 步骤 0.1：检查必备软件

在 PowerShell 中依次执行以下命令，确认环境就绪：

```powershell
# 1. 检查 Docker Desktop 是否已安装
docker --version

# 2. 检查 Python 版本（需要 3.9+）
python --version

# 3. 检查 NVIDIA 驱动和 GPU
nvidia-smi

# 4. 检查 WSL2（Docker Desktop 需要）
wsl --status
```

```
执行顺序与设计意图：
你从最基础的依赖开始检查：Docker 是所有大数据组件的基础容器平台；
Python 是 RAG 后端和 Flink 作业的解释器；
nvidia-smi 确认你的 GPU 可被 Ollama 用于推理加速；
WSL2 是 Docker Desktop 在 Windows 上的后端引擎。

如果某个命令报错，请先安装对应的软件：
- Docker Desktop: https://www.docker.com/products/docker-desktop/
- Python 3.11+: https://www.python.org/downloads/ (安装时勾选 Add to PATH)
- NVIDIA 驱动: 通过 GeForce Experience 更新到最新版
- WSL2: 以管理员运行 PowerShell → wsl --install
```

#### 步骤 0.2：确认项目目录结构

```powershell
# 进入项目根目录
cd D:\Pycharm_Project\SQL-Agent

# 查看目录结构（确认一切完整）
ls
```

预期输出：
```
SQL-Agent/           # 主项目（含 Phase 1 和 Phase 2 的 Docker 配置）
rag_local/           # 本地 RAG 问答系统（独立 Python 项目）
```

---

## 第一部分：数据管道部署（Phase 1 + Phase 2 Docker）

---

### 步骤 1：配置 Docker 资源

Docker Desktop 默认内存分配可能不足。大数据组件（Kafka、Flink、Doris）总共需要约 10-12GB，需要提前调整。

**操作：**
1. 打开 Docker Desktop → Settings → Resources
2. 设置 WSL Integration > Memory 为 **16384 MB** (16GB)
3. 设置 Swap 为 **2048 MB**
4. 点击 Apply & Restart

```
为什么需要 16GB？

查看 docker-compose-phase2.yml 中各容器的内存限制：
  ZooKeeper:   512m      ← 协调服务，轻量
  Kafka:       2g        ← 消息队列，缓存消息需要内存
  Flink JM:    2g        ← JobManager 管理作业
  Flink TM:    4g        ← TaskManager 执行作业（最大消耗者）
  Doris FE:    2g        ← 查询前端
  Doris BE:    4g        ← 数据节点（第二大消耗者）
  合计:       ~14.5g     ← 接近 16GB，所以宿主机内存越大越好

这些容器共享宿主机的内存。如果内存不足，容器会被 OOM Killer 杀掉，
表现为 "退出代码 137" 或容器反复重启。
```

---

### 步骤 2：启动所有 Docker 服务

**操作：**

```powershell
# 进入 Phase 2 目录（Phase 2 的 docker-compose 包含 Phase 1 的所有服务 + Doris）
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase2_doris

# 启动全部容器（后台运行）
docker compose -f docker-compose-phase2.yml up -d
```

```
这条命令干了什么？

docker compose up -d 会读取 docker-compose-phase2.yml 并启动所有容器。
-d 表示后台运行（detached），不会阻塞终端。

启动顺序由 depends_on 控制（对比 yml 文件中的配置）：
  ① ZooKeeper (zk)     ← 先启动，Kafka 依赖它做服务发现
  ② Kafka              ← 等待 ZK 就绪后启动
  ③ Flink JobManager   ← 和 Kafka 并行启动
  ④ Flink TaskManager  ← 等待 JM 就绪
  ⑤ Doris FE           ← 和 Flink 并行启动（有 healthcheck）
  ⑥ Doris BE           ← 等待 FE 健康检查通过

依赖链不是串行的——Kafka 和 Flink 是并行启动的，节约时间。
Docker Compose 会监控容器的退出状态，如果某个容器启动失败，自动停止整个堆栈。
```

**验证所有容器正常运行：**

```powershell
# 查看容器状态
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

预期输出：
```
NAMES       STATUS                  PORTS
zk          Up About a minute       0.0.0.0:2181->2181/tcp
kafka       Up About a minute       0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
flink-jm    Up About a minute       0.0.0.0:8081->8081/tcp, 0.0.0.0:6123->6123/tcp
flink-tm    Up About a minute       0.0.0.0:6124->6124/tcp
doris-fe    Up About a minute (healthy)  0.0.0.0:8030->8030/tcp, 0.0.0.0:9030->9030/tcp
doris-be    Up About a minute       0.0.0.0:9050->9050/tcp
```

```
重点关注：
- STATUS 列应该是 "Up" 开头，Doris FE 应有 "(healthy)" 标记
- Doris BE 启动最慢（需要等待 FE 健康），可能需要 1-2 分钟
- 如果某个容器反复重启（Restarting），用 docker logs <容器名> 查看原因
  最常见原因：内存不足，容器被 OOM 杀掉
```

**如果只想启动 Phase 1（不启动 Doris）：**

```powershell
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase1_paimon
docker compose -f docker-compose-phase1.yml up -d
```

> Phase 2 的 docker-compose 是 Phase 1 的超集（包含所有 Phase 1 的服务 + Doris 的 FE 和 BE），所以直接使用 Phase 2 即可，不需要先启动 Phase 1 再叠加 Phase 2。

---

### 步骤 3：等待服务就绪并创建 Kafka Topic

虽然 Docker 容器已启动，但服务内部的初始化还需要时间。我们需要等服务进程真正开始监听端口后，再执行后续操作。

**操作：**

```powershell
# 等待 Flink WebUI 就绪（Flink 的 REST API 端口 8081）
Write-Host "等待 Flink WebUI..."
do {
    Start-Sleep -Seconds 3
    try { $r = curl -s http://localhost:8081; $ok = $? } catch { $ok = $false }
} while (-not $ok)
Write-Host "Flink WebUI 就绪！"

# 等待 Kafka 就绪后创建 topic
Write-Host "创建 Kafka topic..."
docker exec kafka kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

# 验证 topic 已创建
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# 等待 Doris FE 就绪
Write-Host "等待 Doris FE..."
do {
    Start-Sleep -Seconds 5
    $health = docker inspect --format='{{.State.Health.Status}}' doris-fe 2>$null
} while ($health -ne "healthy")
Write-Host "Doris FE 就绪！"
```

```
这段脚本的执行逻辑：

第一阶段：等待 Flink WebUI
  curl http://localhost:8081 尝试连接 Flink 的 REST API。
  因为容器启动后 Flink 还需要初始化 JobManager 的 RPC 系统和 Web 服务器，
  所以用循环轮询直到端口响应。
  Flink WebUI 地址: http://localhost:8081，启动后可以浏览器打开查看作业状态。

第二阶段：创建 Kafka Topic
  kafka-topics.sh 是 Kafka 自带的命令行管理工具。
  --topic orders      创建一个名为 orders 的 topic
  --partitions 3       分成 3 个分区，提高并行消费能力
  --replication-factor 1  副本数 1（单机部署无需多副本）
  这里的 orders topic 是数据管道的"入口"：模拟订单数据会发到这个 topic，
  Flink 作业会从这个 topic 消费数据。
  --if-not-exists 防止重复执行报错。

第三阶段：等待 Doris FE
  与 Flink 不同，Docker Compose 已经为 Doris FE 配置了 healthcheck。
  docker inspect 读取容器的健康状态。Doris FE 的 healthcheck 命令是：
    mysql -h localhost -P 9030 -uroot -e "SELECT 1"
  当这条 SQL 能成功执行时，说明 FE 已完全就绪。
```

---

### 步骤 4：下载并安装 Paimon JAR

Paimon 不是独立服务，而是 Flink 的一个连接器 JAR。Flink 需要加载这个 JAR 才能读写 Paimon 表。

**为什么要手动下载？**
Docker 镜像中预装的 Flink 不包含 Paimon 依赖。Paimon 是 Apache 的子项目，它的 Flink 连接器需要额外下载。在我们的 Docker Compose 中，如果要打包成自包含镜像需要定制 Dockerfile，手动复制 JAR 是更简单的方式。

**操作：**

```powershell
# 步骤 4.1：下载 Paimon Flink 连接器 JAR
# 0.7.0 是 Flink 1.18 对应的 Paimon 版本
# 使用阿里云 Maven 镜像（国内访问更快）
wget https://maven.aliyun.com/repository/public/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar -OutFile paimon-flink-1.18-0.7.0.jar

# 如果阿里云镜像下载失败，使用 Maven 中央仓库：
# wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar

# 步骤 4.2：将 JAR 复制到 Flink JobManager 容器的 lib 目录
docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/

# 步骤 4.3：复制到 TaskManager 容器（两个容器各有一份 Flink 安装）
docker cp paimon-flink-1.18-0.7.0.jar flink-tm:/opt/flink/lib/

# 步骤 4.4：重启 Flink 集群以加载新 JAR
docker restart flink-jm flink-tm

# 步骤 4.5：等待 Flink 重启完成
Start-Sleep -Seconds 15
Write-Host "Paimon JAR 安装完成！"
```

```
深入理解这 4 步：

为什么要复制到 JobManager 和 TaskManager？
Flink 采用主从架构：
  JobManager (flink-jm):  负责接收作业、生成执行计划
  TaskManager (flink-tm): 负责执行计算任务
JobManager 解析 SQL 时需要 Paimon Catalog 实现，
TaskManager 读写 Paimon 文件时需要 Paimon 文件格式实现。
两只各有一份 Flink 安装，所以 JAR 要复制到两个容器。

为什么复制 JAR 后要重启 Flink？
JAR 只在 Flink 启动时加载一次。Flink 的 classloader 在运行期间
不会扫描新增的 JAR 文件。docker restart 会向 Flink 进程发送 SIGTERM，
让它优雅关闭，然后重新启动进程并加载 lib/ 目录下的所有 JAR。

重启后验证 JAR 是否加载成功：
  docker exec flink-jm ls /opt/flink/lib/ | grep paimon
  → 应该看到 paimon-flink-1.18-0.7.0.jar

如何验证 JAR 工作正常？
  进入 Flink SQL Client 创建 Paimon Catalog，不报错说明 JAR 加载成功：
  docker exec -it flink-jm sql-client.sh
  然后在 SQL Client 中执行：CREATE CATALOG paimon_catalog WITH (...);
  如果报 ClassNotFoundException，说明 JAR 没加载成功。
```

---

### 步骤 5：验证数据管道

现在基础设施已就绪，我们来验证每个组件是否工作正常。

#### 5.1 验证 Flink SQL Client

```powershell
# 进入 Flink SQL Client（交互式命令行）
docker exec -it flink-jm sql-client.sh
```

在 SQL Client 中执行：
```sql
-- 创建 Paimon Catalog（Paimon 的元数据管理入口）
CREATE CATALOG paimon_catalog WITH (
    'type' = 'filesystem',
    'warehouse' = 'file:///opt/paimon/data/warehouse'
);

-- 使用 Catalog
USE CATALOG paimon_catalog;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- 创建订单表
CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3),
    proc_time     AS PROCTIME(),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input'
);

-- 验证表创建成功
SHOW TABLES;

-- 退出 SQL Client
EXIT;
```

```
这些 SQL 的作用对后续部署的影响：

CREATE CATALOG 告诉 Flink"去哪里读写 Paimon 表"，
warehouse 路径 = file:///opt/paimon/data/warehouse
这个路径映射到宿主机的 ../data/paimon/，也就是 SQL-Agent/data/paimon/
所以数据持久在宿主机上，重启容器不会丢失。

orders 表定义了字段和主键。注意：
  - PRIMARY KEY (order_id): Paimon 是"主键表"而非追加表
  - bucket=4: 数据分为 4 个桶存储，提高读写并行度
  - changelog-producer=input: Paimon 根据输入的变更记录自动生成 changelog

这个表是所有后续操作的基础——Kafka 数据 → Flink → Paimon 表的写入目标。
Phase 2 中 Doris 的 Paimon Catalog 也会读取这张表。
```

#### 5.2 验证 Doris

```powershell
# 使用 MySQL 客户端连接 Doris（Doris 兼容 MySQL 协议，端口 9030）
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW PROC '/backends';"
```

预期输出包含一行 Alive: true 的 BE 节点。

```
深入理解 Doris 验证：

Doris 有两个核心组件：
  FE (Frontend): 查询前端，处理 SQL 解析、元数据管理、查询计划生成
    端口 9030 = MySQL 协议（任何 MySQL 客户端都能连）
    端口 8030 = WebUI（浏览器打开 http://localhost:8030）

  BE (Backend): 数据节点，负责数据存储和查询执行
    SHOW PROC '/backends' 显示所有 BE 节点的状态

Alive: true 表示 BE 正常运行且已注册到 FE。
如果 BE 为 Dead，检查 doris-be 容器日志：
  docker logs doris-be
  
Doris 验证后，创建基础的数据库用于后续操作：
  docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "CREATE DATABASE IF NOT EXISTS lakehouse;"
```

#### 5.3 开启 Flink 流式写入（验证数据流转）

```powershell
# 进入 Flink SQL Client 开始流式写入
docker exec -it flink-jm sql-client.sh
```

在 SQL Client 中执行：
```sql
-- 设置 checkpoint（Paimon 依赖 checkpoint 提交数据）
SET 'execution.checkpointing.interval' = '30s';

-- 创建 DataGen 源表（模拟数据生成器，每秒 5 条）
CREATE TEMPORARY TABLE datagen_orders (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '1000000',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '1001',
    'fields.user_id.end' = '9999',
    'fields.product_name.length' = '10',
    'fields.category.length' = '6',
    'fields.quantity.min' = '1',
    'fields.quantity.max' = '10',
    'fields.unit_price.min' = '10',
    'fields.unit_price.max' = '1000',
    'fields.order_ts.kind' = 'random',
    'fields.order_ts.max-past' = '3600000'
);

-- 启动流式写入（持续运行的 Flink 作业）
INSERT INTO orders SELECT * FROM datagen_orders;
```

```
这条命令启动了一个持续运行的 Flink 流作业。
执行后不要关闭 SQL Client——让它持续运行。

关键机制：
1. DataGen 是 Flink 内置的连接器，每秒生成 5 条模拟订单数据
2. INSERT INTO orders 是一个流式语句，会持续运行不会自动结束
3. 每 30 秒（checkpoint 间隔），Flink 将数据提交到 Paimon
4. Paimon 在 checkpoint 时创建一个 snapshot

此时可以打开 Flink WebUI: http://localhost:8081
在 Running Jobs 中可以看到这个流式作业的状态
```

#### 5.4 验证 Paimon 中的数据

新开一个 PowerShell 窗口，创建另一个 SQL Client 连接来查询数据：

```powershell
docker exec -it flink-jm sql-client.sh
```

在第二个 SQL Client 中：
```sql
USE CATALOG paimon_catalog;
USE demo;

-- 设置为批模式（查完就结束，不是持续监听）
SET 'execution.runtime-mode' = 'batch';

-- 查询数据
SELECT COUNT(*) AS total_orders FROM orders;

SELECT category, COUNT(*) AS cnt, SUM(total_amount) AS revenue
FROM orders
GROUP BY category
ORDER BY revenue DESC;

-- 退出
EXIT;
```

```
如果 total_orders > 0，说明整条数据管道验证通过：
  DataGen → 生成数据 → Flink 流式写入 → Paimon 表 → 可查询

至此，Phase 1 的核心链路已打通。你可以继续到 Phase 2 的结果验证，
也可以在完成核心链路后退出 SQL Client。
查询为 0 的最常见原因：checkpoint 还没触发（需要等到第一个 30 秒 checkpoint 完成）。
```

#### 5.5 验证 Doris 读取 Paimon 数据

```powershell
# 在 Doris 中注册 Paimon Catalog
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
);
"

# 查询 Paimon 中的数据（通过 Doris）
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
SWITCH TO paimon_catalog.demo;
SELECT category, COUNT(*) AS cnt, SUM(total_amount) AS revenue
FROM orders
GROUP BY category
ORDER BY revenue DESC;
"
```

```
Doris Paimon Catalog 实现了"湖仓一体"的核心功能：
  零 ETL（无需数据复制），Doris 直接读取 Paimon 的文件。

原理：
  Doris FE 中的 Paimon Catalog 实现了 Paimon 文件的读取逻辑。
  当执行 SWITCH TO paimon_catalog.demo 后，
  Doris 不再使用自己的存储引擎，而是直接读取 Paimon warehouse 中的
  数据文件（/opt/paimon/data/warehouse/demo.db/orders/）。

  这意味着：
  - 不需要把数据从 Paimon 复制到 Doris
  - Paimon 中写入的新数据 Doris 立即可见
  - 缺点是查询性能受限于文件读取速度（相比 Doris 原生存储）

如果查询为空，说明 Docker 容器映射的 warehouse 路径不一致。
Flink 容器将 warehouse 映射到宿主机的 ../data/paimon/，
Doris 容器也需要能访问到同一路径。确保两个容器中 /opt/paimon/data/
都指向同一份数据。
```

---

### 步骤 6：运行实时数据流

前面的 DataGen 只是模拟数据，现在我们连接真实的 Kafka 数据流。

**操作**（保持第一步的流式写入 SQL Client 运行中，新开窗口）：

```powershell
# 安装 Python 依赖（在宿主机上）
pip install kafka-python faker

# 运行 Kafka 生产者（发送模拟订单数据到 Kafka）
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase1_paimon
python flink-job/kafka_producer.py
```

你应该看到：
```
Connecting to Kafka at localhost:29092...
Sending orders to topic 'orders'...
Press Ctrl+C to stop.

Sent order #1: Wireless Headphones x3 = $123.45 [Electronics]
Sent order #2: Green Tea x7 = $45.67 [Food]
...
```

```
Kafka Producer 的工作流：

kafka_producer.py 做的事情：
  1. 连接 localhost:29092（Kafka 的外部监听端口）
  2. 每秒生成 0.5-2 条随机订单数据（带真实分布的商品和分类）
  3. 发送到 Kafka 的 orders topic

如果你想在 Flink 中消费 Kafka 数据而不是 DataGen，执行：
  docker exec -it flink-jm sql-client.sh
  然后执行 03_streaming_write.sql 中的 Kafka 连接语句（已注释）。

但注意：DataGen 和 Kafka 是两种数据源，不需要同时运行。
上面我们已经在用 DataGen 写入了，所以 Kafka 生产者产生的是独立数据。
实际使用时选择一种即可。

当前推荐的做法：
  保持 DataGen 流式写入运行（简单稳定），
  Kafka Producer 作为学习 Kafka 写入方式的演示。
```

---

### 步骤 7：Doris 双写与查询验证

这一节演示"双写模式"：Flink 将数据同时写入 Paimon（湖存储）和 Doris（查询加速）。

**操作：**

```powershell
# 在 Doris 中创建目标表
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
USE lakehouse;
CREATE TABLE IF NOT EXISTS doris_orders (
    order_id      BIGINT,
    user_id       BIGINT,
    category      STRING,
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      DATETIME
) DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES ('replication_num' = '1');
"
```

```
创建 Doris 目标表的要点：

CREATE TABLE 语法：
  - DISTRIBUTED BY HASH(order_id) BUCKETS 4
    Doris 是 MPP 架构，数据按 order_id 哈希分布到 4 个桶
    这决定了 Doris 内部的数据分布和查询并行度
  
  - replication_num = '1'
    单机部署时副本数为 1。生产环境通常为 3。

注意：Doris 的 DDL 不通过 Flink SQL Client 执行，
而是直接通过 MySQL 协议发给 Doris FE（端口 9030）。
因为 Doris 有自己的元数据管理系统，不依赖 Flink。
```

**提交双写 Flink 作业：**

```powershell
# 提交 dual_writer.py 到 Flink 集群
docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py
```

```
dual_writer.py 的作业逻辑（对照源码阅读）：

整体流程：
  Kafka Source → 双路 Sink ─→ Paimon (湖存储，数据底座)
                           └→ Doris  (查询加速，毫秒级响应)

关键代码分析：
 ① 注册 Paimon Catalog
    table_env.execute_sql("CREATE CATALOG paimon_catalog WITH (...)")
    告诉 Flink 怎么连接到 Paimon warehouse

 ② 创建 Kafka Source 表
    CREATE TEMPORARY TABLE kafka_orders WITH ('connector' = 'kafka', ...)
    TEMPORARY 意味着表定义只在当前会话有效，不会持久化到 Catalog。
    设置 scan.startup.mode = 'latest-offset' → 只消费新数据。

 ③ 创建 Doris Sink 表
    CREATE TEMPORARY TABLE doris_sink WITH ('connector' = 'doris', ...)
    使用 Flink Doris Connector 将数据通过 Stream Load 协议写入 Doris。
    'fenodes' = 'doris-fe:8030' → Doris FE 的 HTTP 端口（不是 MySQL 端口）

 ④ StatementSet 同时提交两个 INSERT
    stmt_set.add_insert_sql("INSERT INTO orders SELECT * FROM kafka_orders")
    stmt_set.add_insert_sql("INSERT INTO doris_sink SELECT ... FROM kafka_orders")
    
    关键：StatementSet 把两个 INSERT 合并为一个 Flink 作业。
    这样 Kafka 的一条消息只需要读取一次，同时写入两个系统。
    如果写成两个独立的 executeSql()，Flink 会创建两个作业，
    Kafka 数据会被消费两次，浪费资源且可能产生不一致。
```

**验证双写数据：**

```powershell
# 验证 Doris 中的数据
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
USE lakehouse;
SELECT COUNT(*) AS doris_total FROM doris_orders;
"

# 对比 Paimon 中的数据
docker exec flink-jm sql-client.sh
```

在 SQL Client 中：
```sql
USE CATALOG paimon_catalog;
USE demo;
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS paimon_total FROM orders;
EXIT;
```

```
通常 Doris 中的数据条数 ≤ Paimon 中的数据条数，原因：
  - dual_writer.py 从 Kafka 消费（latest-offset），只消费作业启动后的消息
  - 之前的 DataGen 数据只写入了 Paimon，不会出现在 Doris 中
  - 只有 dual_writer 作业启动后 Kafka 收到的消息才会双写

这验证了"两条写入链路"的概念：
  链路 1（我们正用着）: DataGen → Flink SQL → Paimon
  链路 2（刚刚启动）: Kafka → dual_writer.py → Paimon + Doris
  两条链路是独立的，互不干扰。
```

---

### 步骤 8：一键部署脚本（可选替代上述手动步骤）

如果觉得逐步操作太繁琐，可以直接使用项目提供的启动脚本。

**Phase 1 快速启动：**

```powershell
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase1_paimon
.\start-phase1.sh
```

**Phase 2 快速启动：**

```powershell
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase2_doris
.\start-phase2.sh
```

```
start-phase2.sh 自动完成了以上所有步骤：
  Step 1: docker compose up -d
  Step 2: 轮询等待 Flink WebUI + Doris FE 就绪
  Step 3: 执行 init-kafka-topics.sh 创建 orders topic
  Step 4: 验证 Doris 是否工作
  
但脚本不会自动下载 Paimon JAR（因为需要用户自行选择下载源），
你仍然需要执行"步骤 4"中的 JAR 下载命令。

如果不想每次启动都手动复制 JAR，可以考虑：
  1. 下载 JAR 到项目目录
  2. 修改 docker-compose，用 volumes 挂载方式自动加载
```

---

## 第二部分：本地 RAG 问答系统部署

---

### 步骤 9：安装 Ollama 并下载模型

**Ollama** 是一个"LLM 管家"。它帮你管理 AI 模型的下载、加载和运行，并提供 HTTP API。

```
为什么需要 Ollama？
传统的 AI 模型推理需要你手动配置 Python、CUDA、PyTorch 等复杂环境。
Ollama 把这些打包成"一行命令"：
  ollama pull qwen3.5:35b-a3b  → 自动下载 10GB 的模型，自动配置 GPU 推理

Ollama 的本质：
  一个本地的、REST API 风格的"模型服务平台"。
  它启动后监听 localhost:11434，提供 HTTP API。
  我们的 app.py 通过 HTTP 调用 Ollama，不需要直接操作 GPU。
```

**操作：**

```powershell
# 步骤 9.1：下载 Ollama 安装包（在浏览器中操作）
# 访问 https://ollama.com/download 下载 Windows 版
# 或者使用 winget：
winget install Ollama.Ollama

# 步骤 9.2：安装完成后，系统托盘会出现 Ollama 图标
# 打开 PowerShell 验证安装
ollama --version
```

```
安装 Ollama 的过程：
① 安装程序将 Ollama 复制到 C:\Program Files\Ollama
② 注册为 Windows 服务，开机自启
③ 启动服务，监听 http://127.0.0.1:11434
```

```powershell
# 步骤 9.3：下载 BGE-M3 嵌入模型
# 这个模型将"文本"变成"向量"，用于文档的语义检索
ollama pull bge-m3
```

```
BGE-M3 (~2.2GB) 下载过程：
  pulling manifest    ← 获取模型清单（包含模型的文件列表和哈希值）
  pulling xxx...      ← 逐个下载模型分片（多个 .safetensors 文件）
  verifying sha256    ← 验证每个文件完整性（防止下载损坏）
  writing manifest    ← 记录已下载到本地 .ollama/models/ 目录
  success

为什么需要 BGE-M3？
  LLM（如 Qwen）不能直接"记住"你的文档内容。
  RAG 的流程是：文档分块 → 每块转成向量（BGE-M3）→ 存入向量数据库
  用户提问 → 问题转成向量 → 在数据库中找最相似的块 → 发给 LLM 回答

  BGE-M3 的特殊优势：支持多语言（中英日韩等）、1024 维向量、速度快。
```

```powershell
# 步骤 9.4：下载 Qwen3.5-35B-A3B 问答模型
# 这是阿里通义千问的 MoE 模型，总参数 35B，活跃参数 3B
ollama pull qwen3.5:35b-a3b
```

```
Qwen3.5-35B-A3B (~10GB) 是 RAG 的"大脑"。

为什么选这个模型？
  MoE（混合专家）架构：总参数 35B，但推理时只激活 3B。
  这意味着：
  - 显存占用 = 加载完整 35B 的量化参数 (~10GB)
  - 推理速度 = 接近 3B 模型的速度（因为只激活 3B）
  - 回答质量 = 35B 级别的知识量

  RTX 5060 Ti 16GB 可以运行：
  - Qwen3.5 占用 ~10GB
  - BGE-M3 占用 ~2GB
  - 系统开销 ~1GB
  - 合计 ~13GB，有 3GB 富余

如果下载失败（新模型可能还未上架）：
  # 替代方案：使用 Qwen2.5 7B（只需 ~7GB）
  ollama pull qwen2.5:7b
  # 然后修改 app.py 中的 LLM_MODEL = "qwen2.5:7b"
```

```powershell
# 步骤 9.5：确认两个模型都已下载
ollama list
```

预期输出：
```
NAME                     ID              SIZE    MODIFIED
bge-m3:latest            xxxx...         2.2 GB  2 minutes ago
qwen3.5:35b-a3b:latest   xxxx...         10 GB   5 minutes ago
```

```powershell
# 步骤 9.6：验证 Ollama API 正常工作

# 测试 BGE-M3 嵌入功能
# /api/embed: 文本 → 向量
curl.exe http://localhost:11434/api/embed -d "{\"model\": \"bge-m3\", \"input\": \"测试文本\"}"

# 测试 Qwen 对话功能（第一次加载需要 10-30 秒）
curl.exe http://localhost:11434/api/chat -d "{\"model\": \"qwen3.5:35b-a3b\", \"messages\": [{\"role\": \"user\", \"content\": \"你好\"}], \"stream\": false}"
```

```
测试 BGE-M3：
  API: POST /api/embed
  参数: {"model": "bge-m3", "input": "要转向量的文本"}
  返回: {"embeddings": [[0.123, -0.456, ...]]}
  该返回的 embeddings 是一个 1024 维浮点数向量。

测试 Qwen：
  API: POST /api/chat
  参数: {"model": "qwen3.5:35b-a3b", "messages": [...], "stream": false}
  返回: {"message": {"role": "assistant", "content": "回答内容..."}}
  
  第一次请求会慢（从 SSD 加载 ~10GB 模型到显存），后续请求秒回。
  
  stream: false 表示等完整回答返回。
  stream: true（app.py 中的方式）会让 LLM 逐字返回，实现"打字"效果。

注意：curl.exe（带 .exe 后缀）是 Windows 内置的 curl，
只是 curl（无后缀）在 PowerShell 中可能被映射为 Invoke-WebRequest。
```

---

### 步骤 10：部署 RAG Python 后端

```powershell
# 步骤 10.1：进入 rag_local 目录
cd D:\Pycharm_Project\SQL-Agent\rag_local

# 步骤 10.2：创建 Python 虚拟环境
# 为什么需要虚拟环境？隔离依赖包，避免和系统 Python 或其他项目冲突
python -m venv .venv

# 步骤 10.3：激活虚拟环境
.venv\Scripts\activate
# 激活后，命令行前面会显示 (.venv)

# 步骤 10.4：安装依赖包
# -r requirements.txt 表示从文件读取包列表
pip install -r requirements.txt

# 如果下载慢，使用清华镜像：
# pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

```
requirements.txt 中的依赖及其作用：

fastapi          → Web 框架，提供 HTTP API（FastAPI 是最新最流行的 Python Web 框架）
uvicorn          → FastAPI 的 ASGI 服务器（类似 Java 的 Tomcat）
chromadb         → 向量数据库，存储文档向量，支持相似度搜索
httpx            → HTTP 客户端，用于调用 Ollama API（比 requests 更现代，支持 async）
PyMuPDF          → PDF 解析库（fitz 引擎，中文支持好，速度快）
python-docx      → Word 文档解析（读取 .docx 文件内容）
openpyxl         → Excel 解析（读取 .xlsx 文件，逐行处理）
markdown         → Markdown 解析（移除标记符号，保留纯文本）
langchain-community  → LangChain 的文档加载器集合（统一各格式解析接口）
langchain-text-splitters → 文本切分器（把长文档切成小块）

安装过程需要联网下载这些包及其依赖（如 numpy、pandas 等，总共约 200MB）。
```

---

### 步骤 11：准备文档并启动 RAG

```powershell
# 步骤 11.1：打开 watch_folder（把文档放进去）
start D:\Pycharm_Project\SQL-Agent\rag_local\src\watch_folder
```

```
支持的文件格式：
  .pdf    → 文字型 PDF（不支持扫描件/图片型 PDF）
  .docx   → Word 文档
  .xlsx   → Excel 表格（每行作为一个独立文档块）
  .csv    → CSV 文件
  .md     → Markdown 文件（推荐，结构清晰）
  .txt    → 纯文本文件（必须 UTF-8 编码）

建议先用简单的 .md 文件测试，确认流程跑通后再放真正的文档。
```

```powershell
# 步骤 11.2：启动 RAG 系统
python app.py
```

```
启动过程中，日志解释了每一步在做什么：

[初始化] Ollama 地址: http://localhost:11434
  → 创建 OllamaClient 实例，准备调用 Ollama API

[OK] 嵌入模型: bge-m3
  → 调用 GET /api/tags，确认 BGE-M3 模型已下载

[OK] 问答模型: qwen3.5:35b-a3b
  → 同样通过 /api/tags 确认 Qwen 已就绪

[索引] 扫描目录: D:\...\src\watch_folder
  → 调用 load_all_documents() 扫描目录下所有支持的文件

[OK] PDF 解析完成: xxx.pdf (12 页)
[OK] DOCX 解析完成: xxx.docx
  → 每种格式都被对应的解析器处理

[索引] 共加载 21 个文档片段
  → 所有解析出的 Document 对象总数

[索引] 文档被切分为 45 个文本块
  → RecursiveCharacterTextSplitter 将长文档切成 ~500 字的块

[索引] 正在调用 BGE-M3 生成向量（共 45 块）...
[进度] 5/45
[进度] 45/45
  → 每个文本块调 BGE-M3 转为 1024 维向量

[OK] 索引完成！共 45 个文本块已存入 ChromaDB
  → 向量 + 原文存入 chroma_db/ 目录

[服务] 启动完成！浏览器打开: http://localhost:8000
  → FastAPI 服务就绪，可以开始提问了
```

---

### 步骤 12：使用 RAG 问答系统

**操作：** 打开浏览器访问 `http://localhost:8000`

```
你会看到一个清新的聊天界面。这是 src/static/index.html 渲染的 Web UI。
它通过 Server-Sent Events (SSE) 与后端通信，实现"逐字输出"效果。
```

**一些问题示例：**
- "文档中主要讲了什么？"
- "帮我总结一下核心内容"
- "关于 XX 问题，文档是怎么说的？"

```
一次完整问答的内部流程（对照 app.py 阅读）：

① 用户在浏览器输入 → 前端 fetch /ask/stream?query=...

② 后端 search_similar(query) 执行：
   a) ollama.embed_text("问题文本") → BGE-M3 → 向量 v
   b) chroma_collection.query(v, n_results=4) → 找最相似的 4 个块
   c) 返回 [{"text": "块内容", "source": "来源文件.pdf", "score": 0.92}, ...]

③ 后端 build_rag_prompt(query, chunks) 构建 Prompt：
   System: "你是一个基于本地知识库的智能问答助手..."
   User:   "参考文档1: ...\n参考文档2: ...\n问题: ..."

④ ollama.chat_stream(messages) → 调 Qwen /api/chat stream=true
   → 逐字返回 → SSE 推送给浏览器

⑤ 浏览器把每个字追加到气泡 → 用户看到"打字"效果

⑥ 最后发送来源信息 → 显示"参考来源：xxx.pdf (92%)"
```

---

### 步骤 13：状态检查与常见命令

```powershell
# 查看 Docker 容器状态
docker ps -a

# 查看 Flink Web 界面（浏览器打开）
start http://localhost:8081

# 查看 Doris Web 界面
start http://localhost:8030

# 查看 Flink 中的 Paimon 数据
docker exec flink-jm sql-client.sh
# 进入后: USE CATALOG paimon_catalog; USE demo;
#         SET 'execution.runtime-mode' = 'batch';
#         SELECT COUNT(*) FROM orders;

# 查看 Doris 中的数据
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "USE lakehouse; SELECT COUNT(*) FROM doris_orders;"

# 查看 RAG 系统状态
curl.exe http://localhost:8000/stats

# 查看 Ollama 模型列表
ollama list

# 查看 GPU 显存使用（RAG 运行时重要）
nvidia-smi
```

---

## 第三部分：全栈验证清单

完成所有部署后，用这个清单确认每个组件正常工作：

### ✅ 数据管道验证

| 组件 | 验证命令 | 成功标志 |
|------|---------|---------|
| ZooKeeper | `docker ps \| findstr zk` | Up |
| Kafka | `docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092` | 列出 orders |
| Flink | `curl -s http://localhost:8081 \| findstr "version"` | 返回 Flink 版本信息 |
| Paimon | `docker exec flink-jm sql-client.sh -e "USE CATALOG paimon_catalog; USE demo; SET 'execution.runtime-mode'='batch'; SELECT COUNT(*) FROM orders;"` | 返回行数 > 0 |
| Doris FE | `docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1;"` | 返回 1 |
| Doris BE | `docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW PROC '/backends';"` | 有 Alive=true 的 BE |
| Doris Paimon Catalog | `docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SWITCH TO paimon_catalog.demo; SELECT COUNT(*) FROM orders;"` | 返回行数 > 0 |

### ✅ RAG 问答验证

| 组件 | 验证命令 | 成功标志 |
|------|---------|---------|
| Ollama | `curl.exe http://localhost:11434/api/tags` | 返回模型列表（含 bge-m3 和 qwen3.5） |
| RAG API | `curl.exe http://localhost:8000/stats` | 返回索引统计 |
| RAG WebUI | 浏览器打开 http://localhost:8000 | 看到聊天界面 |
| 问答测试 | 在界面中输入问题 | 收到带来源引用的回答 |

---

## 第四部分：停止与清理

```powershell
# 停止 Docker 服务（保留数据）
cd D:\Pycharm_Project\SQL-Agent\SQL-Agent\phase2_doris
docker compose -f docker-compose-phase2.yml down

# 停止 Docker 服务并删除数据卷（清空所有数据）
# docker compose -f docker-compose-phase2.yml down -v

# 停止 RAG 系统（在运行 app.py 的窗口按 Ctrl+C）

# 停止 Ollama
# 系统托盘 → Ollama 图标 → Quit

# 删除 RAG 虚拟环境和向量数据库（可选，完全清理）
# cd D:\Pycharm_Project\SQL-Agent\rag_local
# deactivate
# Remove-Item .venv -Recurse -Force
# Remove-Item chroma_db -Recurse -Force
```

---

## 附录：架构图与数据流

```
                               用户提问 → http://localhost:8000 (Web UI)
                                    │
                    ┌───────────────┴───────────────┐
                    │    RAG 问答系统 (Python)        │
                    │    app.py                       │
                    │         │                       │
                    │    ┌────┴────┐                  │
                    │    │ Ollama  │ → Qwen (回答)     │
                    │    │ HTTP    │ → BGE-M3 (向量)   │
                    │    └────┬────┘                  │
                    │         │                       │
                    │    ┌────┴────┐                  │
                    │    │ChromaDB│ ← 文档向量库       │
                    │    └─────────┘                  │
                    └─────────────────────────────────┘

┌──────────┐    ┌──────────┐    ┌──────────────────┐    ┌─────────────────┐
│  DataGen  │───→│  Flink   │───→│    Paimon 表     │←───│  Doris Catalog  │
│  /Kafka   │    │  SQL     │    │  (湖存储)        │    │  (零 ETL 查询)  │
│  (模拟数据)│    │  /PyFlink│    │                  │    │                  │
└──────────┘    └──────────┘    └──────────────────┘    └─────────────────┘
                      │                                        │
                      │    ┌──────────────────┐                │
                      └───→│  Doris 表        │←───────────────┘
                           │  (查询加速，      │
                           │   MySQL 协议 9030)│
                           └──────────────────┘
                                    │
                                    ↓
                            SQL 客户端 / BI 工具
                            (mysql -h 127.0.0.1 -P 9030 -uroot)
```

---

### 端口速查

| 端口 | 组件 | 用途 |
|------|------|------|
| 2181 | ZooKeeper | Kafka 协调服务 |
| 9092 | Kafka | 容器内数据通道 |
| 29092 | Kafka | 宿主机外部访问 |
| 8081 | Flink WebUI | 作业管理和监控 |
| 6123 | Flink RPC | JobManager 通信 |
| 8030 | Doris WebUI | Doris 管理界面 |
| 9030 | Doris MySQL | MySQL 协议查询 |
| 9050 | Doris BE | 数据节点通信 |
| 8000 | RAG WebUI | 问答聊天界面 |
| 11434 | Ollama API | AI 模型推理服务 |
