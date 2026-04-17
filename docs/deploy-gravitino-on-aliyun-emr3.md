---
title: Deploy Apache Gravitino on Alibaba Cloud EMR 3
slug: /deploy-gravitino-on-aliyun-emr3
license: "This software is licensed under the Apache License version 2."
---

## Overview

This document records a validated deployment path for running Apache Gravitino `1.2.0` on an Alibaba Cloud EMR 3 cluster.

The deployment described here uses:

- Alibaba Cloud EMR 3 master node
- Gravitino binary package `gravitino-1.2.0-bin.tar.gz`
- Java 17 for Gravitino only
- PostgreSQL RDS as the Gravitino relational backend storage
- A non-default HTTP port because port `8090` was already occupied on the target host

This document also records an important limitation found during validation:

- Gravitino can connect to Hive Metastore-backed Iceberg catalogs.
- The online Spark jobs in the validated environment used Alibaba Cloud DLF Iceberg catalog implementation `org.apache.iceberg.aliyun.dlf.hive.DlfCatalog`.
- With that setup, Gravitino could list schemas from HMS-related metadata, but it could not list the online DLF-backed Iceberg tables through a Hive-backed Iceberg catalog definition.

## Deployment assumptions

This deployment assumes:

- EMR 3 services continue to use the existing system Java 8.
- Gravitino uses its own dedicated Java 17 runtime.
- The Gravitino package is unpacked under `/opt/gravitino/gravitino-1.2.0-bin`.
- The Gravitino metadata backend uses PostgreSQL.
- The Gravitino HTTP service runs on port `8091`.

## Prerequisites

Before deployment, prepare the following:

- A Java 17 installation on the EMR master node
- A PostgreSQL database for Gravitino metadata
- PostgreSQL JDBC driver jar copied into `${GRAVITINO_HOME}/libs`
- Network connectivity from the EMR master node to PostgreSQL RDS
- The Gravitino binary package `gravitino-1.2.0-bin.tar.gz`

## Why Java 17 is required

Gravitino `1.0.0` and later require Java 17 at runtime. EMR 3 commonly runs on Java 8 for Hadoop ecosystem services, so the recommended approach is:

- Keep EMR services on Java 8
- Install Java 17 additionally
- Configure only the Gravitino process to use Java 17

Do not switch the system default Java unless you have validated the impact on existing EMR services.

## Install Java 17

Install Java 17 on the EMR master node. For example:

```shell
sudo yum install -y java-17-openjdk java-17-openjdk-devel
```

Verify the installation:

```shell
/usr/lib/jvm/java-17-openjdk/bin/java -version
```

If the actual installation path differs, use the real Java 17 path in the environment configuration below.

## Prepare the installation directory

Create the target directory, extract the package, and ensure the `hadoop` user owns the installation:

```shell
sudo mkdir -p /opt/gravitino
sudo tar -xzf gravitino-1.2.0-bin.tar.gz -C /opt/gravitino
sudo chown -R hadoop:hadoop /opt/gravitino
```

The validated installation path was:

```text
/opt/gravitino/gravitino-1.2.0-bin
```

Set `GRAVITINO_HOME` to the real extracted directory. Do not point it to `/opt/gravitino` if the package contents are actually under `/opt/gravitino/gravitino-1.2.0-bin`.

## Configure `gravitino-env.sh`

Edit `${GRAVITINO_HOME}/conf/gravitino-env.sh`:

```shell
#!/bin/bash

export GRAVITINO_HOME=/opt/gravitino/gravitino-1.2.0-bin

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$JAVA_HOME/bin:$PATH

export GRAVITINO_LOG_DIR=${GRAVITINO_HOME}/logs
export GRAVITINO_PID_DIR=${GRAVITINO_HOME}/run
export GRAVITINO_DATA_DIR=${GRAVITINO_HOME}/data
```

Create the required runtime directories:

```shell
sudo mkdir -p /opt/gravitino/gravitino-1.2.0-bin/logs
sudo mkdir -p /opt/gravitino/gravitino-1.2.0-bin/run
sudo mkdir -p /opt/gravitino/gravitino-1.2.0-bin/data
sudo chown -R hadoop:hadoop /opt/gravitino
```

## Configure PostgreSQL backend storage

Gravitino `1.2.0` supports relational backend storage via JDBC. In this deployment, PostgreSQL RDS was used instead of the default embedded backend.

### Add the PostgreSQL JDBC driver

Copy the PostgreSQL JDBC driver into `${GRAVITINO_HOME}/libs`. For example:

```text
/opt/gravitino/gravitino-1.2.0-bin/libs/postgresql-42.7.4.jar
```

Without this jar, Gravitino fails with:

```text
java.lang.ClassNotFoundException: org.postgresql.Driver
```

### Create the PostgreSQL schema

Run the Gravitino PostgreSQL schema initialization script:

```shell
psql \
  -h <postgres-host> \
  -p <postgres-port> \
  -U <postgres-user> \
  -d <postgres-database> \
  -f /opt/gravitino/gravitino-1.2.0-bin/scripts/postgresql/schema-1.2.0-postgresql.sql
```

In the validated environment, using the wrong port caused connection failures. For Alibaba Cloud PostgreSQL RDS, use the actual internal port shown by the RDS console instead of assuming the default `5432`.

If the schema is not initialized, Gravitino can connect to PostgreSQL but fails with errors like:

```text
ERROR: relation "metalake_meta" does not exist
```

### Configure `gravitino.conf`

Edit `${GRAVITINO_HOME}/conf/gravitino.conf` and set the relational backend properties:

```properties
gravitino.server.shutdown.timeout = 3000

gravitino.server.webserver.host = 0.0.0.0
gravitino.server.webserver.httpPort = 8091
gravitino.server.webserver.minThreads = 24
gravitino.server.webserver.maxThreads = 200
gravitino.server.webserver.stopTimeout = 30000
gravitino.server.webserver.idleTimeout = 30000
gravitino.server.webserver.threadPoolWorkQueueSize = 100
gravitino.server.webserver.requestHeaderSize = 131072
gravitino.server.webserver.responseHeaderSize = 131072

gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend
gravitino.entity.store.relational.jdbcUrl = jdbc:postgresql://<postgres-host>:<postgres-port>/<postgres-database>
gravitino.entity.store.relational.jdbcDriver = org.postgresql.Driver
gravitino.entity.store.relational.jdbcUser = <postgres-user>
gravitino.entity.store.relational.jdbcPassword = <postgres-password>

gravitino.catalog.cache.evictionIntervalMs = 3600000

gravitino.cache.enabled = true
gravitino.cache.maxEntries = 10000
gravitino.cache.expireTimeInMs = 3600000
gravitino.cache.enableStats = false
gravitino.cache.enableWeigher = true
gravitino.cache.implementation = caffeine

gravitino.authorization.enable = false
gravitino.server.visibleConfigs = gravitino.authorization.serviceAdmins
gravitino.authorization.serviceAdmins = anonymous

gravitino.auxService.names =

gravitino.iceberg-rest.classpath = iceberg-rest-server/libs, iceberg-rest-server/conf
gravitino.iceberg-rest.host = 0.0.0.0
gravitino.iceberg-rest.httpPort = 9001
gravitino.iceberg-rest.catalog-backend = memory
gravitino.iceberg-rest.warehouse = /tmp/

gravitino.lance-rest.classpath = lance-rest-server/libs
gravitino.lance-rest.host = 0.0.0.0
gravitino.lance-rest.httpPort = 9101
gravitino.lance-rest.namespace-backend = gravitino
gravitino.lance-rest.gravitino-uri = http://localhost:8091
```

### Notes about this configuration

- Port `8090` was already in use on the target host. Gravitino was moved to `8091`.
- Auxiliary services were disabled during initial deployment to reduce variables.
- The template configuration in the package contains commented SQLite examples. Do not use SQLite unless you also provide a matching JDBC driver jar.

## Start Gravitino

Use the foreground mode for the first startup:

```shell
cd /opt/gravitino/gravitino-1.2.0-bin
./bin/gravitino.sh run
```

If startup succeeds, verify the server:

```shell
curl -v http://127.0.0.1:8091/api/version
```

The validated deployment returned:

```json
{"code":0,"version":{"version":"1.2.0", ...}}
```

After validation, use background mode:

```shell
./bin/gravitino.sh start
```

Stop the service:

```shell
./bin/gravitino.sh stop
```

## Troubleshooting checklist

### `ClassNotFoundException: org.apache.gravitino.server.GravitinoServer`

Cause:

- `GRAVITINO_HOME` pointed to the wrong directory

Fix:

- Set `GRAVITINO_HOME` to the real extracted package directory
- In the validated deployment, this was `/opt/gravitino/gravitino-1.2.0-bin`

### `ClassNotFoundException: org.sqlite.JDBC`

Cause:

- The configuration still referenced SQLite, but no SQLite JDBC driver jar was present

Fix:

- Switch to PostgreSQL or MySQL and place the correct JDBC driver jar into `${GRAVITINO_HOME}/libs`

### `ClassNotFoundException: org.postgresql.Driver`

Cause:

- PostgreSQL JDBC driver jar was missing from `${GRAVITINO_HOME}/libs`

Fix:

- Copy `postgresql-<version>.jar` into `${GRAVITINO_HOME}/libs`

### `PSQLException: The connection attempt failed`

Possible causes:

- Wrong PostgreSQL port
- RDS whitelist or security group issue
- No network connectivity from EMR to RDS

Fix:

- Verify the real internal RDS port
- Confirm whitelist and VPC connectivity
- Test with:

```shell
nc -vz <postgres-host> <postgres-port>
```

### `ERROR: relation "metalake_meta" does not exist`

Cause:

- PostgreSQL schema initialization script was not executed

Fix:

- Run `scripts/postgresql/schema-1.2.0-postgresql.sql`

### `Failed to bind to /0.0.0.0:8090`

Cause:

- Port `8090` was already used by another service

Fix:

- Change `gravitino.server.webserver.httpPort` to another free port such as `8091`

## Hive Metastore-backed Iceberg catalog validation

After Gravitino was started successfully, a metalake and an Iceberg catalog were created through the Web UI using:

- Catalog provider: `Apache Iceberg`
- Catalog backend: `hive`
- URI: the value of `hive.metastore.uris`
- Warehouse: `oss://zego-opt-emr-datalake/warehouse/`

With this configuration, Gravitino could:

- Connect to the Hive Metastore
- List schemas such as `default`, `iceberg`, and `test`

However, Gravitino still could not list the online Iceberg tables that were written by Spark jobs.

## Important limitation: Alibaba Cloud DLF Iceberg catalog

The validated online Spark configuration used:

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aliyun.dlf.hive.DlfCatalog
```

This means the online Spark jobs were using Alibaba Cloud DLF Iceberg catalog implementation instead of a standard Hive Metastore-backed Iceberg catalog.

As a result:

- Gravitino could read schema-level metadata exposed through the Hive-related path
- Gravitino could not list the online DLF-backed Iceberg tables through a Hive-backed Iceberg catalog definition

In other words, the deployment itself was successful, but the online Iceberg table discovery requirement was not met because the metadata catalog implementation used by Spark and the metadata catalog implementation configured in Gravitino were different.

## Recommended conclusion for this deployment

This deployment path is valid for:

- Running Gravitino `1.2.0` on Alibaba Cloud EMR 3
- Using Java 17 only for Gravitino
- Using PostgreSQL as the Gravitino backend storage
- Accessing Gravitino Web UI and core APIs successfully
- Connecting to Hive Metastore-backed catalog metadata

This deployment path does not prove support for:

- Directly browsing online Iceberg tables stored in Alibaba Cloud DLF Iceberg catalog through a Hive-backed Iceberg catalog configuration in Gravitino

If the target requirement is to expose the online Iceberg tables managed by:

```properties
org.apache.iceberg.aliyun.dlf.hive.DlfCatalog
```

you must first confirm whether the target Gravitino version supports Alibaba Cloud DLF-backed Iceberg catalog integration directly.

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=DeployGravitinoOnAliyunEMR3" alt="" />
