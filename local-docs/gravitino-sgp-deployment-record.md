# Singapore Gravitino Deployment & Migration Record

## 1. Deployment Summary

| Item | Value |
|------|-------|
| Server | 10.3.96.74 (master-1-1.c-888acfeb88b653be) |
| Gravitino Version | 1.2.0 |
| Install Path | `/opt/gravitino/gravitino-1.2.0-bin` |
| Java | OpenJDK 17 (`/usr/lib/jvm/java-17-openjdk-17.0.7.0.7-3.0.2.al8.x86_64`) |
| Management Port | 8091 |
| Iceberg REST Port | 9090 (8090 occupied by TEZ Tomcat) |
| Entity Store DB | PostgreSQL `gravitino_entity_store` |
| Iceberg Catalog DB | PostgreSQL `gravitino` |
| PostgreSQL Host | pgm-t4n23n51z1l997w9.pgsql.singapore.rds.aliyuncs.com:5432 |
| PostgreSQL User | gravitino |
| Warehouse | `oss://zego-opt-emr-datalake-sgp/warehouse/iceberg.db/` |
| OSS Access | JindoFS 6.1.0 (via EMR core-site.xml) |

### Key Config Files

- `/opt/gravitino/gravitino-1.2.0-bin/conf/gravitino.conf`
- `/opt/gravitino/gravitino-1.2.0-bin/conf/gravitino-iceberg-rest-server.conf`
- `/opt/gravitino/gravitino-1.2.0-bin/conf/gravitino-env.sh`
- `/opt/gravitino/gravitino-1.2.0-bin/iceberg-rest-server/conf/core-site.xml`

### Service Management

```bash
# Start
/opt/gravitino/gravitino-1.2.0-bin/bin/gravitino.sh start

# Stop
/opt/gravitino/gravitino-1.2.0-bin/bin/gravitino.sh stop

# Restart
/opt/gravitino/gravitino-1.2.0-bin/bin/gravitino.sh restart

# Health check
curl -sf http://localhost:8091/api/version
curl -sf http://localhost:9090/iceberg/v1/config
```

## 2. Deployment Issues & Resolutions

| Issue | Resolution |
|-------|-----------|
| Java 8 incompatible (class version 61.0) | Set `JAVA_HOME` to Java 17 in `gravitino-env.sh` |
| `GRAVITINO_VERSION` not set | Added `export GRAVITINO_VERSION=1.2.0` to `gravitino-env.sh` |
| Port 8090 occupied by TEZ Tomcat | Changed Iceberg REST port to 9090 |
| OSS write failed (wrong bucket) | Changed warehouse from `zego-opt-emr-datalake` to `zego-opt-emr-datalake-sgp` |
| Entity store on H2 (not production-ready) | Migrated to PostgreSQL `gravitino_entity_store`, initialized with `schema-1.2.0-postgresql.sql` |
| PostgreSQL driver missing in main libs | Copied `postgresql-42.6.0.jar` to both `libs/` and `iceberg-rest-server/libs/` |

## 3. Validation Results

### 3.1 Basic Connectivity

| Test | Result |
|------|--------|
| Management endpoint (8091) | ✅ |
| Iceberg REST endpoint (9090) | ✅ |
| PostgreSQL connectivity | ✅ |
| OSS read/write via JindoFS | ✅ |

### 3.2 Spark CRUD via Gravitino

| Test | Result |
|------|--------|
| CREATE NAMESPACE | ✅ |
| CREATE TABLE | ✅ |
| INSERT | ✅ |
| SELECT | ✅ |
| SHOW TABLES | ✅ |

### 3.3 Migration Simulation

| Test | Result |
|------|--------|
| Create table in DLF, register to Gravitino via REST API | ✅ |
| Read historical data through Gravitino | ✅ 3 rows match |
| Write new data through Gravitino | ✅ 4th row added |
| DLF cannot see Gravitino-written data (expected) | ✅ Confirmed |
| format-version preserved (v1) | ✅ No auto-upgrade |
| Iceberg 1.1.0 client + 1.10.1 server compatibility | ✅ Read/write both work |

### 3.4 Batch Registration

| Metric | Value |
|--------|-------|
| Total tables in OSS | 689 |
| Export time (20 parallel) | ~30 seconds |
| Successfully registered | 670 |
| Failed (metadata cleaned by active writes) | 18 |
| Expected to succeed in maintenance window | 688 (all except verify_db) |

## 4. DLF Namespace Inventory (Singapore)

| Namespace | Table Count | Notes |
|-----------|-------------|-------|
| iceberg | 687 | Core production data |
| default | 0 | Empty |
| ocean | 0 | Empty |
| sink_config | 0 | Empty |

Note: Migration docs referenced Shanghai namespaces (`iceberg_stg`, `ocean_stg`, `open_data`) which do not exist in Singapore.

## 5. Migration Scripts

Location: `/opt/gravitino/migration-scripts/`

### fast_export.sh

Exports latest metadata.json path for all tables. Uses 20 parallel `hadoop fs -ls` calls.

```bash
bash /opt/gravitino/migration-scripts/fast_export.sh
# Output: /opt/gravitino/migration-scripts/metadata_export.csv
```

### gravitino_migration.py

Batch operations: create namespaces, register tables, verify.

```bash
# Create namespaces
python3 gravitino_migration.py create_ns iceberg

# Register all tables from export CSV
python3 gravitino_migration.py register iceberg

# Verify table counts match
python3 gravitino_migration.py verify iceberg
```

## 6. Production Cutover Plan

### Prerequisites

- [ ] Confirm 18 failed tables status (active writes expected to resolve in maintenance window)
- [ ] High availability plan (currently single node)
- [ ] Notify Spark/Flink job owners
- [ ] Prepare rollback commands

### Maintenance Window (~15 minutes)

```
Step 1: Confirm Spark batch jobs completed
Step 2: Pause Spark scheduling
Step 3: Flink savepoint and stop
Step 4: Re-run fast_export.sh (get latest metadata)
Step 5: Run register (all tables)
Step 6: Run verify (confirm counts)
Step 7: Modify spark-defaults.conf
Step 8: Modify Flink catalog definition
Step 9: Restart Flink from savepoints
Step 10: Resume Spark scheduling
```

### Spark Config Change

```properties
# REMOVE:
spark.sql.catalog.iceberg.catalog-impl = org.apache.iceberg.aliyun.dlf.hive.DlfCatalog

# ADD:
spark.sql.catalog.iceberg.type = rest
spark.sql.catalog.iceberg.uri = http://10.3.96.74:9090/iceberg/
```

File: `/etc/emr/spark-conf/spark-defaults.conf`

### Flink Config Change

```sql
-- BEFORE (DLF):
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aliyun.dlf.hive.DlfCatalog',
  ...
);

-- AFTER (Gravitino):
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://10.3.96.74:9090/iceberg/',
  'warehouse' = 'oss://zego-opt-emr-datalake-sgp/warehouse/iceberg.db/'
);
```

### Post-Cutover Verification

- [ ] Flink ODS writes landing normally
- [ ] Spark DWD/ADS batch jobs succeed
- [ ] Historical data queries work
- [ ] Gravitino service stable (process, logs)
- [ ] Downstream systems (Trino, BI) working

### Rollback

```bash
# Restore spark-defaults.conf backup
cp /etc/emr/spark-conf/spark-defaults.conf.bak /etc/emr/spark-conf/spark-defaults.conf

# Revert Flink catalog to DLF
# Restart all jobs
```

Retain DLF configuration for at least 7 days after cutover.

## 7. Known Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Single node (no HA) | High | Deploy second node + LB before production cutover |
| Port differs from docs (9090 vs 8090) | Low | Update all references |
| Bucket name differs from docs (sgp suffix) | Low | Already corrected in config |
| 18 tables with large/active metadata | Low | Re-export in maintenance window resolves this |
| Rollback after new writes requires metadata sync | Medium | Keep DLF config 7 days; sync latest metadata.json back if needed |

## 8. Date

Deployment and validation completed: 2026-04-02
