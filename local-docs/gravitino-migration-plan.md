# DLF → Gravitino Iceberg Catalog 迁移方案

## 1. 迁移目标

将 Iceberg 表的元数据管理从阿里云 DLF 迁移到 Gravitino Iceberg REST Server，要求：

- **数据不动**：OSS 上的 Iceberg 数据文件和 metadata 文件不做任何移动或复制
- **平滑迁移**：维护窗口控制在分钟级
- **可灰度**：先迁移非核心 namespace 验证，再迁移核心业务
- **可回滚**：任何阶段都能回退到 DLF，不丢数据
- **用户无感**：迁移后可正常查询所有历史数据

## 2. 现状分析

### 2.1 DLF 侧

| 项目 | 详情 |
|------|------|
| Catalog 类型 | DlfCatalog (`org.apache.iceberg.aliyun.dlf.hive.DlfCatalog`) |
| Namespace | `default`, `iceberg`, `iceberg_stg`, `ocean`, `ocean_stg`, `open_data`, `sink_config`（共 7 个） |
| 表数量 | `iceberg` namespace 约 700+ 张，其他 namespace 待统计 |
| format-version | 全部为 **v1** |
| 数据格式 | Parquet + zstd 压缩 |
| 分区方式 | 大部分为 `day, hour`（identity partition） |
| 数据位置 | `oss://zego-opt-emr-datalake/warehouse/iceberg.db/` |
| IO 实现 | `OSSFileIO`（部分表显式配置）/ JindoFS |

### 2.2 数据链路

```
Flink 集群（实时）          Spark 集群（批处理，1 小时调度）
    │                           │
    │ 写 ODS                    │ ODS → DWD → ADS
    ▼                           ▼
┌─────────────────────────────────────┐
│          DLF Catalog                │
│  iceberg / iceberg_stg / ocean /   │
│  ocean_stg / open_data / sink_config│
└─────────────────────────────────────┘
                  │
                  ▼
     OSS (oss://zego-opt-emr-datalake/warehouse/iceberg.db/)
```

### 2.3 Gravitino 侧（已部署验证通过）

| 项目 | 详情 |
|------|------|
| 版本 | Gravitino 1.2.0 |
| 部署位置 | `10.111.242.233:/opt/gravitino/gravitino-1.2.0-bin` |
| 管理端口 | 8091 |
| Iceberg REST 端口 | 8090 |
| Catalog Backend | JDBC（PostgreSQL RDS） |
| Warehouse | `oss://zego-opt-emr-datalake/warehouse/iceberg.db/`（需与 DLF 一致） |
| 默认 format-version | 1 |
| OSS 访问 | JindoFS（已配置） |

### 2.4 下游依赖排查

迁移前必须确认以下系统是否读写 Iceberg 表，同步切换：

- [ ] Trino（机器上已部署）
- [ ] Kyuubi
- [ ] BI 工具 / 报表系统
- [ ] 数据质量 / 监控系统
- [ ] 其他消费方

## 3. 前置准备（不停服）

### 3.1 统一 Warehouse 路径

将 Gravitino 的 warehouse 设为和 DLF 一致，确保新老表在同一目录结构下：

```bash
# 修改两个配置文件
for CONF in \
  /opt/gravitino/gravitino-1.2.0-bin/conf/gravitino.conf \
  /opt/gravitino/gravitino-1.2.0-bin/conf/gravitino-iceberg-rest-server.conf; do
  sed -i 's|^\(gravitino.iceberg-rest.warehouse\s*=\s*\).*|\1oss://zego-opt-emr-datalake/warehouse/iceberg.db/|' "$CONF"
done
```

重启 Gravitino 生效。

### 3.2 导出 DLF 元数据快照（回滚保底）

```bash
cat > /tmp/export_dlf_metadata.py << 'PYEOF'
import subprocess, json, csv, sys

namespaces = ['default', 'iceberg', 'iceberg_stg', 'ocean', 'ocean_stg', 'open_data', 'sink_config']

with open('/tmp/dlf_metadata_snapshot.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['namespace', 'table_name', 'metadata_location', 'data_location'])

    for ns in namespaces:
        # Get table list
        result = subprocess.run(
            ['spark-sql', '--master', 'local[2]', '-e', f'SHOW TABLES IN iceberg.{ns}'],
            capture_output=True, text=True, timeout=120
        )
        tables = [line.strip() for line in result.stdout.strip().split('\n')
                  if line.strip() and not line.startswith('Time taken')]

        for table in tables:
            # Get metadata location
            result2 = subprocess.run(
                ['spark-sql', '--master', 'local[2]', '-e',
                 f'SHOW CREATE TABLE iceberg.{ns}.{table}'],
                capture_output=True, text=True, timeout=60
            )
            output = result2.stdout + result2.stderr

            # Extract LOCATION
            location = ''
            for line in output.split('\n'):
                if 'LOCATION' in line:
                    location = line.strip().replace("LOCATION '", "").rstrip("'")

            writer.writerow([ns, table, '', location])
            print(f'Exported: {ns}.{table}', file=sys.stderr)

print(f'Done. Saved to /tmp/dlf_metadata_snapshot.csv', file=sys.stderr)
PYEOF

python3 /tmp/export_dlf_metadata.py
```

> 此脚本可能执行较慢（700+ 张表），建议提前运行。产出文件 `/tmp/dlf_metadata_snapshot.csv` 需妥善保存。

### 3.3 生成批量注册脚本

Iceberg 的 `register_table` 需要每张表最新的 metadata.json 路径。从 DLF 获取后生成 SQL：

```bash
cat > /tmp/generate_register_sql.py << 'PYEOF'
import subprocess, re, sys

namespaces = ['default', 'iceberg', 'iceberg_stg', 'ocean', 'ocean_stg', 'open_data', 'sink_config']

# Output SQL files per namespace for灰度
for ns in namespaces:
    result = subprocess.run(
        ['spark-sql', '--master', 'local[2]', '-e', f'SHOW TABLES IN iceberg.{ns}'],
        capture_output=True, text=True, timeout=120
    )
    tables = [line.strip() for line in result.stdout.strip().split('\n')
              if line.strip() and not line.startswith('Time taken')]

    with open(f'/tmp/register_{ns}.sql', 'w') as f:
        # Create namespace
        f.write(f"CREATE NAMESPACE IF NOT EXISTS gvt_iceberg.{ns};\n\n")

        for table in tables:
            # Get table location
            result2 = subprocess.run(
                ['spark-sql', '--master', 'local[2]', '-e',
                 f'SHOW CREATE TABLE iceberg.{ns}.{table}'],
                capture_output=True, text=True, timeout=60
            )
            output = result2.stdout + result2.stderr
            location = ''
            for line in output.split('\n'):
                if 'LOCATION' in line:
                    location = line.strip().replace("LOCATION '", "").rstrip("'")

            if location:
                # Find latest metadata.json in the metadata dir
                metadata_dir = f"{location}/metadata"
                f.write(f"-- {ns}.{table}\n")
                f.write(f"CALL gvt_iceberg.system.register_table(table => '{ns}.{table}', metadata_file => '<LATEST_METADATA_JSON>');\n")
                f.write(f"-- metadata_dir: {metadata_dir}\n\n")

            print(f'Generated: {ns}.{table}', file=sys.stderr)

    print(f'Saved: /tmp/register_{ns}.sql', file=sys.stderr)
PYEOF

python3 /tmp/generate_register_sql.py
```

> **注意：** `register_table` 需要精确的 metadata.json 文件路径，不是目录。需要额外一步从 OSS 上找到每张表 metadata 目录下最新的 `.metadata.json` 文件。可以用 `hadoop fs -ls` 或 `ossutil` 获取：

```bash
# 获取某张表最新的 metadata.json
hadoop fs -ls oss://zego-opt-emr-datalake/warehouse/iceberg.db/ods_billing_streamstat/metadata/*.metadata.json \
  | sort -k6,7 | tail -1 | awk '{print $NF}'
```

### 3.4 批量获取所有表的最新 metadata.json 路径

```bash
cat > /tmp/resolve_metadata.sh << 'BASH_EOF'
#!/bin/bash
# 输入：/tmp/dlf_metadata_snapshot.csv
# 输出：/tmp/register_all.sql

echo "-- Auto-generated register_table SQL" > /tmp/register_all.sql
echo "" >> /tmp/register_all.sql

CURRENT_NS=""

while IFS=',' read -r ns table_name _ location; do
    # Skip header
    [ "$ns" = "namespace" ] && continue
    [ -z "$location" ] && continue

    # Create namespace if changed
    if [ "$ns" != "$CURRENT_NS" ]; then
        echo "" >> /tmp/register_all.sql
        echo "-- ========== Namespace: $ns ==========" >> /tmp/register_all.sql
        echo "CREATE NAMESPACE IF NOT EXISTS gvt_iceberg.$ns;" >> /tmp/register_all.sql
        echo "" >> /tmp/register_all.sql
        CURRENT_NS="$ns"
    fi

    # Find latest metadata.json
    LATEST=$(hadoop fs -ls "${location}/metadata/" 2>/dev/null \
        | grep "\.metadata\.json$" \
        | sort -k6,7 \
        | tail -1 \
        | awk '{print $NF}')

    if [ -n "$LATEST" ]; then
        echo "CALL gvt_iceberg.system.register_table(table => '${ns}.${table_name}', metadata_file => '${LATEST}');" >> /tmp/register_all.sql
    else
        echo "-- WARNING: No metadata.json found for ${ns}.${table_name} at ${location}/metadata/" >> /tmp/register_all.sql
    fi

    echo "Resolved: ${ns}.${table_name}" >&2

done < /tmp/dlf_metadata_snapshot.csv

echo "Done. Output: /tmp/register_all.sql" >&2
BASH_EOF

chmod +x /tmp/resolve_metadata.sh
bash /tmp/resolve_metadata.sh
```

### 3.5 准备验证 SQL

```bash
cat > /tmp/verify_migration.sql << 'SQLEOF'
-- 对比 DLF 和 Gravitino 的表数量
-- 在 DLF 上执行
-- SHOW TABLES IN iceberg.iceberg;
-- 在 Gravitino 上执行
SHOW TABLES IN gvt_iceberg.iceberg;

-- 抽查几张核心表的数据量
SELECT count(*) as cnt, 'ods_billing_streamstat' as tbl FROM gvt_iceberg.iceberg.ods_billing_streamstat WHERE day = '${CHECK_DAY}';
SELECT count(*) as cnt, 'dwd_speedlog' as tbl FROM gvt_iceberg.iceberg.dwd_speedlog WHERE day = '${CHECK_DAY}';

-- 查看 snapshot 是否完整
SELECT * FROM gvt_iceberg.iceberg.ods_billing_streamstat.snapshots ORDER BY committed_at DESC LIMIT 5;
SQLEOF
```

## 4. 灰度迁移

### 4.1 灰度策略

按 namespace 分批迁移，从低风险到高风险：

| 批次 | Namespace | 风险等级 | 说明 |
|------|-----------|---------|------|
| 第 1 批 | `sink_config`, `open_data` | 低 | 配置表/开放数据，影响面小 |
| 第 2 批 | `iceberg_stg`, `ocean_stg` | 中 | STG 环境，验证完整链路 |
| 第 3 批 | `ocean` | 中 | 业务数据 |
| 第 4 批 | `iceberg`, `default` | 高 | 核心生产数据，700+ 张表 |

### 4.2 第 1 批灰度执行（示例）

**目的：** 用 `sink_config` 和 `open_data` 验证注册 + 读取流程，不涉及实时写入。

```bash
# 1. 注册（不需要停服，这些表没有实时写入）
spark-sql \
  --jars /opt/gravitino-test-jars/iceberg-spark-runtime-3.3_2.12-1.1.0-1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gvt_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gvt_iceberg.type=rest \
  --conf spark.sql.catalog.gvt_iceberg.uri=http://localhost:8090/iceberg/ \
  --master local[2] \
  -f /tmp/register_sink_config.sql

# 2. 验证：对比 DLF 和 Gravitino 的数据
spark-sql \
  --jars /opt/gravitino-test-jars/iceberg-spark-runtime-3.3_2.12-1.1.0-1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gvt_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gvt_iceberg.type=rest \
  --conf spark.sql.catalog.gvt_iceberg.uri=http://localhost:8090/iceberg/ \
  --master local[2] \
  -e "
SELECT count(*) FROM gvt_iceberg.sink_config.<some_table>;
SELECT count(*) FROM iceberg.sink_config.<some_table>;
-- 两个结果应一致
"
```

**验证通过后再进行下一批。**

### 4.3 第 2~3 批灰度

与第 1 批流程相同。如果涉及 Flink/Spark 写入的表，按第 5 节的维护窗口流程执行。

### 4.4 第 4 批（核心迁移）

见第 5 节。

## 5. 核心迁移执行（第 4 批）

### 5.1 迁移时间线

```
选择 Spark 批作业调度间隙（如整点任务跑完后的 xx:20~xx:50）

xx:00  Spark 小时级批作业触发
xx:20  确认 Spark 作业全部跑完
       ┌─────── 维护窗口开始 ───────┐
xx:20  │ ① 暂停 Spark 调度          │
xx:21  │ ② 停 Flink 作业 (savepoint)│
xx:23  │ ③ 执行批量 register_table  │
xx:28  │ ④ 验证注册结果             │
xx:30  │ ⑤ 切换所有作业 catalog 配置│
xx:35  │ ⑥ 启动 Flink (从 savepoint)│
xx:36  │ ⑦ 恢复 Spark 调度          │
       └─────── 维护窗口结束 ───────┘
xx+1:00 下一轮 Spark 批作业正常触发（走 Gravitino）
```

### 5.2 具体执行步骤

#### ① 暂停 Spark 调度

在调度系统（如 Airflow、DolphinScheduler）中暂停所有 Iceberg 相关的 Spark 任务。

#### ② 停 Flink 作业

```bash
# 对每个 Flink 作业做 savepoint 后停止
# 示例（根据实际作业 ID 替换）
flink stop <job-id> --savepointPath hdfs:///flink/savepoints/
```

> 记录每个作业的 savepoint 路径，后续恢复时需要。

#### ③ 批量注册

```bash
# 执行前面准备好的注册脚本
spark-sql \
  --jars /opt/gravitino-test-jars/iceberg-spark-runtime-3.3_2.12-1.1.0-1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gvt_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gvt_iceberg.type=rest \
  --conf spark.sql.catalog.gvt_iceberg.uri=http://localhost:8090/iceberg/ \
  --master local[2] \
  -f /tmp/register_all.sql 2>&1 | tee /tmp/register_result.log

# 检查有没有失败的
grep -i "error\|fail\|exception" /tmp/register_result.log
```

#### ④ 验证注册结果

```bash
cat > /tmp/verify_count.sql << 'SQLEOF'
-- 对比每个 namespace 的表数量
SHOW TABLES IN gvt_iceberg.iceberg;
SHOW TABLES IN gvt_iceberg.iceberg_stg;
SHOW TABLES IN gvt_iceberg.ocean;
SHOW TABLES IN gvt_iceberg.ocean_stg;

-- 抽查核心表最新分区数据
SELECT count(*) FROM gvt_iceberg.iceberg.ods_billing_streamstat WHERE day = date_format(current_date(), 'yyyy-MM-dd');
SELECT count(*) FROM gvt_iceberg.iceberg.dwd_speedlog WHERE day = date_format(current_date(), 'yyyy-MM-dd');
SQLEOF

spark-sql \
  --jars /opt/gravitino-test-jars/iceberg-spark-runtime-3.3_2.12-1.1.0-1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gvt_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gvt_iceberg.type=rest \
  --conf spark.sql.catalog.gvt_iceberg.uri=http://localhost:8090/iceberg/ \
  --master local[2] \
  -f /tmp/verify_count.sql
```

**如果验证失败 → 直接跳到第 7 节回滚。**

#### ⑤ 切换作业 catalog 配置

**Spark 作业：**

```properties
# 旧配置（DLF）
spark.sql.catalog.iceberg = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl = org.apache.iceberg.aliyun.dlf.hive.DlfCatalog

# 新配置（Gravitino）
spark.sql.catalog.iceberg = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type = rest
spark.sql.catalog.iceberg.uri = http://<gravitino-host>:8090/iceberg/
# 删除 catalog-impl 配置
```

> **注意 catalog 名保持 `iceberg` 不变**（不用 `gvt_iceberg`），这样所有 SQL 中的 `iceberg.xxx.yyy` 不需要改，用户无感。前面测试时用 `gvt_iceberg` 是为了避免和 EMR 全局配置冲突，正式迁移时需要**同时删除 EMR 的 DlfCatalog 配置**。

修改 Spark 全局配置：

```bash
# 修改 spark-defaults.conf
SPARK_CONF=/etc/emr/spark-conf/spark-defaults.conf
cp $SPARK_CONF ${SPARK_CONF}.bak.$(date +%Y%m%d%H%M)

# 替换 catalog 配置
sed -i 's|spark.sql.catalog.iceberg.catalog-impl.*|spark.sql.catalog.iceberg.type rest|' $SPARK_CONF
# 添加 REST URI
grep -q "spark.sql.catalog.iceberg.uri" $SPARK_CONF || \
  echo "spark.sql.catalog.iceberg.uri http://<gravitino-host>:8090/iceberg/" >> $SPARK_CONF
```

**Flink 作业：**

```sql
-- 旧配置
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aliyun.dlf.hive.DlfCatalog',
  ...
);

-- 新配置
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://<gravitino-host>:8090/iceberg/',
  'warehouse' = 'oss://zego-opt-emr-datalake/warehouse/iceberg.db/'
);
```

#### ⑥ 启动 Flink

```bash
# 从 savepoint 恢复
flink run -s <savepoint-path> <flink-job.jar>
```

#### ⑦ 恢复 Spark 调度

在调度系统中恢复所有 Iceberg 相关的 Spark 任务。

### 5.3 迁移后观察

迁移完成后持续观察 **至少 24 小时**：

- [ ] Flink ODS 写入正常（查看最新分区数据）
- [ ] Spark DWD/ADS 批作业正常执行
- [ ] 历史数据查询正常
- [ ] Gravitino 服务稳定（进程、日志无异常）
- [ ] 下游系统（Trino、BI 等）查询正常

## 6. Catalog 名策略

### 方案 A：保持 catalog 名不变（推荐）

```
迁移前：iceberg.iceberg.ods_billing_streamstat  (DLF)
迁移后：iceberg.iceberg.ods_billing_streamstat  (Gravitino)
```

**优点：** 所有 SQL 代码零改动，用户完全无感
**操作：** 修改 `spark-defaults.conf` 中 `spark.sql.catalog.iceberg` 的实现，从 DlfCatalog 切到 REST

### 方案 B：使用新 catalog 名

```
迁移前：iceberg.iceberg.ods_billing_streamstat  (DLF)
迁移后：gvt_iceberg.iceberg.ods_billing_streamstat  (Gravitino)
```

**优点：** 可以同时访问新老 catalog 做对比验证
**缺点：** 所有 SQL 都要改表名，工作量巨大

**建议采用方案 A，** 灰度验证阶段可以临时用 `gvt_iceberg` 做并行对比。

## 7. 回滚方案

### 7.1 回滚触发条件

- 注册后验证发现大量表数据不一致
- Flink 恢复后写入失败
- Spark 批作业跑失败
- Gravitino 服务不稳定

### 7.2 回滚步骤

#### 场景 A：维护窗口内回滚（还没有新数据写入）

```bash
# 1. 还原 Spark 配置
cp /etc/emr/spark-conf/spark-defaults.conf.bak.* /etc/emr/spark-conf/spark-defaults.conf

# 2. Flink 作业用原配置从 savepoint 恢复
flink run -s <savepoint-path> <original-flink-job.jar>

# 3. 恢复 Spark 调度
# DLF 元数据完好无损，一切如旧
```

#### 场景 B：运行一段时间后回滚（已有新数据通过 Gravitino 写入）

```bash
# 1. 停所有作业

# 2. 从 Gravitino PG 中导出每张表最新的 metadata.json 路径
PGPASSWORD='<password>' psql -h <pg-host> -p 1921 -U gravitino -d gravitino_iceberg_catalog -c \
  "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables;" \
  > /tmp/gravitino_latest_metadata.csv

# 3. 将最新的 metadata.json 路径更新到 DLF
#    对每张表执行：
#    ALTER TABLE iceberg.<ns>.<table> SET TBLPROPERTIES('metadata_location' = '<latest_path>')
#    或者用 DLF API 更新

# 4. 还原 Spark/Flink 配置为 DLF
# 5. 重启所有作业
```

> **关键点：** Gravitino 写入的新数据（metadata.json + parquet 文件）都在 OSS 上，不会因为回滚而丢失。只需要让 DLF 的指针指向最新的 metadata.json 即可。

### 7.3 保底恢复

如果一切方法都失败，使用前置准备阶段保存的 `/tmp/dlf_metadata_snapshot.csv`，逐表恢复 DLF 中的表注册。

## 8. Gravitino 生产化加固

迁移前必须完成以下加固，否则不建议切生产流量：

### 8.1 高可用

DLF 是托管服务、天然高可用。Gravitino 目前单节点部署，需要：

```
方案 A：多实例 + 负载均衡
  Gravitino-1 (:8090) ──┐
                         ├── SLB/Nginx (:8090) ← Spark/Flink 连这里
  Gravitino-2 (:8090) ──┘
  （共享同一个 PostgreSQL）

方案 B：主备 + 心跳切换
  Gravitino-primary (:8090)  ← 正常服务
  Gravitino-standby (:8090)  ← 心跳检测，primary 挂了自动切
```

### 8.2 监控告警

```bash
# 进程存活检查
curl -sf http://localhost:8091/api/version || echo "Gravitino DOWN"

# REST 端点可用性
curl -sf http://localhost:8090/iceberg/v1/config || echo "REST Server DOWN"

# PostgreSQL 连接检查
PGPASSWORD='<pwd>' pg_isready -h <pg-host> -p 1921 -U gravitino
```

建议接入现有监控系统（Prometheus + Grafana / 云监控）。

### 8.3 认证授权

当前 Gravitino 使用 `anonymous` 无认证模式。生产环境建议开启认证：

```properties
# gravitino.conf
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin_user
```

### 8.4 备份

PostgreSQL 元数据定期备份：

```bash
# 每天备份 Gravitino 元数据
pg_dump -h <pg-host> -p 1921 -U gravitino gravitino_iceberg_catalog > /backup/gravitino_$(date +%Y%m%d).sql
```

## 9. 完整 Checklist

### 迁移前

- [ ] Gravitino 部署、配置、验证通过（参考部署文档）
- [ ] Warehouse 路径与 DLF 一致：`oss://zego-opt-emr-datalake/warehouse/iceberg.db/`
- [ ] 默认 format-version = 1
- [ ] JindoFS 依赖已添加到 REST Server
- [ ] 导出 DLF 元数据快照（保底）
- [ ] 生成批量 register_table SQL 并验证
- [ ] 准备好新的 Spark/Flink 配置
- [ ] 确认所有下游系统
- [ ] 灰度批次 1~3 验证通过
- [ ] Gravitino 高可用方案就绪
- [ ] 监控告警配置完成
- [ ] PostgreSQL 备份策略配置
- [ ] 回滚方案演练通过
- [ ] 确定维护窗口时间，通知相关方

### 迁移中

- [ ] 确认 Spark 批作业全部跑完
- [ ] 暂停 Spark 调度
- [ ] Flink 做 savepoint 并停止
- [ ] 执行批量 register_table
- [ ] 验证注册结果（表数量、数据抽查）
- [ ] 切换 Spark/Flink 配置
- [ ] 启动 Flink（从 savepoint 恢复）
- [ ] 恢复 Spark 调度
- [ ] 切换下游系统（Trino 等）

### 迁移后

- [ ] 观察 24 小时，确认所有链路正常
- [ ] Flink ODS 写入正常
- [ ] Spark DWD/ADS 正常产出
- [ ] 历史数据查询正常
- [ ] 下游系统正常
- [ ] Gravitino 服务稳定
- [ ] 保留 DLF 配置和元数据 **至少 7 天**，确认无问题后再清理
