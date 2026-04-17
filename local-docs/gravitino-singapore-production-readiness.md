# Singapore Production Rollout for DLF Replacement

## 1. Goal

Deploy a production Gravitino Iceberg REST service in Singapore and replace DLF as the Iceberg
catalog backend with a controlled, reversible rollout.

Success criteria:

- Existing OSS data and Iceberg metadata files stay in place.
- Spark, Flink, and downstream readers can read historical data after the cutover.
- Real-time writes and scheduled batch jobs continue to work after the cutover.
- The production catalog name remains `iceberg` after the final switch.
- Rollback can be completed within the maintenance window if verification fails.

## 2. Source of Truth

- The validated deployment and connectivity procedure is
  `local-docs/gravitino-iceberg-spark-guide.html`.
- This document is the production rollout checklist built on top of that validated guide.
- During gray validation, use `gvt_iceberg` to avoid conflict with the existing EMR global
  `iceberg` catalog bound to DLF.
- During final cutover, replace the existing `iceberg -> DLF` mapping with `iceberg -> REST`.
  Do not keep both configurations for the same catalog name at the same time.

## 3. Architecture Decision

### 3.1 Validation Stage

Use a temporary Spark and Flink catalog name:

- `gvt_iceberg`

Purpose:

- Validate Gravitino without changing existing production SQL.
- Compare DLF and Gravitino side by side.
- Avoid the `both type and catalog-impl are set` conflict.

### 3.2 Final Production Stage

Keep the production-facing catalog name:

- `iceberg`

Method:

- Remove the DLF-specific `catalog-impl` configuration for `spark.sql.catalog.iceberg`.
- Replace it with REST catalog configuration pointing to Gravitino.
- Apply the same principle to Flink jobs and any other engine using the Iceberg catalog.

## 4. Production Scope

The rollout is not complete unless all of the following are verified:

- Gravitino server process and Iceberg REST endpoint
- PostgreSQL backend
- OSS access through JindoFS
- Spark ad hoc SQL
- Spark scheduled jobs
- Flink streaming jobs
- Namespace and table registration
- Historical data reads
- New writes after cutover
- Snapshot visibility and metadata evolution
- Downstream readers such as Trino, Kyuubi, BI, and monitoring systems
- Rollback path

## 5. Preconditions

### 5.1 Deployment Inputs

Fill in and freeze the following before rollout:

| Item | Value |
|------|------|
| Region | Singapore |
| Gravitino version | `1.2.0` |
| Gravitino host(s) | `<sg-gravitino-host>` |
| Management endpoint | `http://<sg-gravitino-host>:8091` |
| Iceberg REST endpoint | `http://<sg-gravitino-host>:8090/iceberg/` |
| PostgreSQL endpoint | `<sg-pg-host>:<sg-pg-port>` |
| PostgreSQL database | `<sg-pg-db>` |
| Warehouse | `oss://zego-opt-emr-datalake/warehouse/iceberg.db/` |
| Spark runtime jar | `iceberg-spark-runtime-3.3_2.12-1.4.3.jar` |
| Maintenance window | `<yyyy-mm-dd hh:mm ~ hh:mm>` |
| Rollback owner | `<name>` |
| Validation owner | `<name>` |

### 5.2 Service Configuration

Both files must be updated consistently:

- `/opt/gravitino/gravitino-1.2.0-bin/conf/gravitino.conf`
- `/opt/gravitino/gravitino-1.2.0-bin/conf/gravitino-iceberg-rest-server.conf`

Required checks:

- `gravitino.iceberg-rest.catalog-backend = jdbc`
- `gravitino.iceberg-rest.uri = jdbc:postgresql://...`
- `gravitino.iceberg-rest.jdbc-driver = org.postgresql.Driver`
- `gravitino.iceberg-rest.jdbc-user = ...`
- `gravitino.iceberg-rest.jdbc-password = ...`
- `gravitino.iceberg-rest.jdbc-initialize = true`
- `gravitino.iceberg-rest.warehouse = oss://zego-opt-emr-datalake/warehouse/iceberg.db/`

### 5.3 Runtime Dependencies

The REST service must contain:

- PostgreSQL JDBC driver
- JindoFS jars required by OSS access
- Hadoop `core-site.xml` with the correct OSS credentials and filesystem settings

### 5.4 Downstream Inventory

No cutover should begin until the owner list is complete:

- Spark scheduled jobs
- Flink streaming jobs
- Trino
- Kyuubi
- BI/reporting systems
- Monitoring and data quality jobs
- Any scripts using `spark-sql`, `spark-submit`, or Flink SQL directly

## 6. Pre-Cutover Validation

### 6.1 Gravitino Health

Run on the Singapore deployment host:

```bash
curl -sf http://<sg-gravitino-host>:8091/api/version
curl -sf http://<sg-gravitino-host>:8090/iceberg/v1/config
```

Expected:

- Both commands return successfully.
- The REST config endpoint returns a non-empty JSON payload.

### 6.2 PostgreSQL Reachability

```bash
PGPASSWORD='<password>' pg_isready -h <sg-pg-host> -p <sg-pg-port> -U <sg-pg-user>
```

Expected:

- PostgreSQL reports ready.

### 6.3 OSS and JindoFS Reachability

Validate from the Gravitino host:

```bash
hadoop fs -ls oss://zego-opt-emr-datalake/warehouse/iceberg.db/
```

Expected:

- Existing warehouse paths are visible.
- No `ClassNotFoundException`, `Failed to get file system`, or auth errors.

### 6.4 Spark Validation with Temporary Catalog

Use `gvt_iceberg` before production cutover:

```bash
spark-sql \
  --jars /opt/gravitino-test-jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gvt_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gvt_iceberg.type=rest \
  --conf spark.sql.catalog.gvt_iceberg.uri=http://<sg-gravitino-host>:8090/iceberg/ \
  --master local[2]
```

Run and verify:

```sql
CREATE NAMESPACE IF NOT EXISTS gvt_iceberg.verify_db;
SHOW NAMESPACES IN gvt_iceberg;

CREATE TABLE IF NOT EXISTS gvt_iceberg.verify_db.verify_table (
  id BIGINT,
  name STRING,
  created_at TIMESTAMP
) USING iceberg;

INSERT INTO gvt_iceberg.verify_db.verify_table VALUES
  (1, 'hello', TIMESTAMP '2026-04-01 10:00:00'),
  (2, 'world', TIMESTAMP '2026-04-01 11:00:00');

SELECT * FROM gvt_iceberg.verify_db.verify_table;
SHOW TABLES IN gvt_iceberg.verify_db;
SELECT * FROM gvt_iceberg.verify_db.verify_table.snapshots ORDER BY committed_at DESC LIMIT 5;
```

Expected:

- Namespace creation succeeds.
- Table creation succeeds.
- Insert succeeds.
- Query returns the inserted rows.
- Snapshot metadata is visible.

### 6.5 Flink Validation with Temporary Catalog

Use the same REST endpoint:

```sql
CREATE CATALOG gvt_iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://<sg-gravitino-host>:8090/iceberg/',
  'warehouse' = 'oss://zego-opt-emr-datalake/warehouse/iceberg.db/'
);
```

Verify:

- `SHOW DATABASES`
- `SHOW TABLES`
- Read one known table
- Write one test table or one isolated test partition

### 6.6 Table Registration Dry Run

Before the production window:

- Export DLF metadata snapshot
- Generate `register_table` SQL
- Execute registration for at least one low-risk namespace
- Compare DLF and `gvt_iceberg` results

Required checks:

- Table count matches
- Spot-check row counts match
- Snapshots can be queried
- No path drift away from the expected OSS warehouse

## 7. Production Cutover Plan

### 7.1 Maintenance Window Entry Criteria

All must be true:

- Singapore Gravitino validation completed
- One or more low-risk namespaces validated via `gvt_iceberg`
- Rollback commands prepared and reviewed
- Spark and Flink owners on standby
- Downstream system owners notified
- DLF metadata snapshot exported and stored safely

### 7.2 Cutover Sequence

1. Confirm the last scheduled Spark batch completed successfully.
2. Pause Spark scheduling.
3. Stop Flink jobs with savepoints.
4. Execute the prepared `register_table` SQL for the production namespaces.
5. Verify table counts and spot-check critical tables through `gvt_iceberg`.
6. Change engine configuration from DLF-backed `iceberg` to REST-backed `iceberg`.
7. Restart Flink from savepoints.
8. Resume Spark scheduling.
9. Validate downstream readers.

## 8. Engine-Specific Switch

### 8.1 Spark Final Switch

Before cutover, `iceberg` is still bound to DLF in EMR global config.

After cutover, the target state must be:

```properties
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=rest
spark.sql.catalog.iceberg.uri=http://<sg-gravitino-host>:8090/iceberg/
```

And the DLF binding must be removed:

```properties
spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aliyun.dlf.hive.DlfCatalog
```

Do not leave both `type=rest` and `catalog-impl=...DlfCatalog` under the same catalog name.

### 8.2 Flink Final Switch

Before cutover:

```sql
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aliyun.dlf.hive.DlfCatalog',
  ...
);
```

After cutover:

```sql
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://<sg-gravitino-host>:8090/iceberg/',
  'warehouse' = 'oss://zego-opt-emr-datalake/warehouse/iceberg.db/'
);
```

## 9. Production Verification Matrix

### 9.1 Control Plane

- `curl /api/version`
- `curl /iceberg/v1/config`
- Gravitino process exists
- Logs contain no repeated exceptions after startup

### 9.2 Metadata Plane

- Registered namespace count matches expectation
- Critical table count matches DLF
- `DESCRIBE TABLE` works for representative tables
- `snapshots` metadata table can be queried

### 9.3 Data Plane

- Historical read on representative ODS table
- Historical read on representative DWD table
- Historical read on representative ADS table
- New Flink write lands successfully after restart
- New Spark write or overwrite succeeds
- Newly written data is visible through follow-up reads

### 9.4 Downstream Plane

- Trino can list schemas and query one representative table
- Kyuubi can access the new catalog path
- BI/report consumers can run at least one existing query successfully
- Data quality and monitoring jobs still read expected partitions

### 9.5 Operational Plane

- CPU, memory, and GC are stable on the Gravitino host
- PostgreSQL connections remain healthy
- Error rate remains flat after enabling production traffic
- Savepoint recovery succeeded for all Flink jobs

## 10. Rollback Plan

Rollback triggers:

- Registration verification fails
- Flink jobs cannot recover cleanly
- Spark production jobs fail after the switch
- Downstream readers cannot query critical datasets
- Gravitino service is unstable under production traffic

Rollback steps:

1. Pause Spark scheduling again.
2. Stop Flink jobs.
3. Restore Spark catalog config from REST back to DLF.
4. Restore Flink catalog config from REST back to DLF.
5. Restart Flink from the saved savepoints using the DLF configuration.
6. Resume Spark scheduling.
7. Validate representative reads on DLF.

Rollback rule:

- Keep DLF metadata and configuration intact for at least 7 days after cutover.

## 11. Acceptance Criteria

The Singapore rollout is considered complete only if:

- All critical engines have switched from DLF to Gravitino successfully.
- Production SQL still works with catalog name `iceberg`.
- No unresolved data mismatch exists in the validation sample set.
- At least one post-cutover Flink write and one post-cutover Spark read/write cycle succeed.
- Downstream readers are validated.
- Rollback remains executable for the agreed retention period.

## 12. Execution Checklist

### Before Maintenance Window

- [ ] Singapore Gravitino hosts deployed
- [ ] PostgreSQL configured and reachable
- [ ] JindoFS and `core-site.xml` in place
- [ ] Both Gravitino config files updated consistently
- [ ] `curl /api/version` passes
- [ ] `curl /iceberg/v1/config` passes
- [ ] OSS warehouse path reachable
- [ ] `gvt_iceberg` Spark validation passes
- [ ] `gvt_iceberg` Flink validation passes
- [ ] Low-risk namespace registration validated
- [ ] DLF metadata snapshot exported
- [ ] Rollback commands prepared
- [ ] System owners notified

### During Maintenance Window

- [ ] Spark schedule paused
- [ ] Flink savepoints completed
- [ ] Production registration SQL executed
- [ ] Table counts verified
- [ ] Critical data spot-check verified
- [ ] Spark `iceberg` switched from DLF to REST
- [ ] Flink `iceberg` switched from DLF to REST
- [ ] Flink jobs restarted successfully
- [ ] Spark scheduling resumed
- [ ] Downstream readers validated

### After Cutover

- [ ] Observe for at least 24 hours
- [ ] Flink writes continue normally
- [ ] Spark hourly jobs continue normally
- [ ] Historical reads still work
- [ ] No unresolved Gravitino or PostgreSQL instability
- [ ] DLF retained for rollback window
