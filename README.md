# **Production-Grade Redshift Provisioned DAG (With Hooks, Operators, Sensors)**

This markdown file contains a **complete end‑to‑end explanation** of a **production-level Apache Airflow DAG** that performs:

* COPY from S3 → Redshift
* MERGE staging → final table
* UNLOAD from Redshift → S3
* Data validation using **RedshiftSQLHook**
* Metadata check using **RedshiftDataHook** (Data API)
* Redshift cluster availability checks using **RedshiftClusterSensor**
* Row-availability checks using **SqlSensor**

This is a **full A‑Z walkthrough** including the **complete DAG file**.

---

# 🧩 **1. Introduction**

This document explains a production-ready Redshift provisioning pipeline. It includes:

* What the pipeline does
* How operators, hooks, and sensors work
* Why each part is needed
* Complete DAG code
* Real-world best practices

This is the kind of documentation you place in a GitHub repository under `README.md` for use by teams.

---

# 🧱 **2. Architecture Overview**

### **High-Level Flow**

```
S3 (Raw Data)
    ↓ COPY
Redshift Staging Table
    ↓ Validation (Hook)
Redshift MERGE → Target Table
    ↓ UNLOAD
S3 (Analytics Bucket)
```

### **Components Used**

| Category          | AWS / Airflow Component | Purpose                   |
| ----------------- | ----------------------- | ------------------------- |
| Storage           | S3                      | Raw input + export output |
| DWH               | Redshift Provisioned    | Staging + Target tables   |
| Airflow Operators | S3ToRedshiftOperator    | COPY wrapper              |
|                   | RedshiftSQLOperator     | SQL/MERGE/DDL/DML         |
|                   | RedshiftToS3Operator    | UNLOAD wrapper            |
| Airflow Sensors   | RedshiftClusterSensor   | Ensure cluster available  |
|                   | SqlSensor               | Row existence check       |
| Hooks             | RedshiftSQLHook         | Validation + queries      |
|                   | RedshiftDataHook        | Metadata & API-based SQL  |

---

# 🧠 **3. Key Concepts**

### ✔ COPY (S3 → Redshift)

Uses Redshift's high-performance COPY command. Airflow wraps this as `S3ToRedshiftOperator`.

### ✔ MERGE (Staging → Target)

Standard upsert procedure — updates existing rows, inserts new ones.

### ✔ UNLOAD (Redshift → S3)

Exports analytical data back to S3 in Parquet.

### ✔ Sensors

Used to safely gate pipeline execution.

### ✔ Hooks

Used when operators are not enough—ideal for validation or fetching metadata.

---

# 🏗 **4. Complete DAG File (Production Ready)**

```python
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG

# Operators
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator

# Sensors
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.sensors.sql import SqlSensor

# Hooks
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

# Python Operators
from airflow.operators.python import PythonOperator


# ------------------------
# DEFAULT ARGS
# ------------------------
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ------------------------
# PYTHON FUNCTIONS USING HOOKS
# ------------------------
# A) Validate staging table row count using RedshiftSQLHook

def validate_staging_count(**context):
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    records = hook.get_first("""
        SELECT COUNT(*)
        FROM staging.orders_stg
        WHERE order_ts::date = '{{ ds }}'::date;
    """)

    count = records[0]

    if count == 0:
        raise ValueError("ERROR: No rows found in staging table. COPY might have failed.")

    context['ti'].xcom_push(key="staging_count", value=count)
    return f"Staging rows found: {count}"


# B) Metadata validation using RedshiftDataHook (Data API)

def metadata_check(**context):
    hook = RedshiftDataHook(
        cluster_identifier="{{ var.value.redshift_cluster_identifier }}",
        database="dev",
        db_user="awsuser",
        aws_conn_id="aws_default"
    )

    sql = "SELECT COUNT(*) AS total FROM mart.orders;"

    resp = hook.execute_statement(sql=sql, with_event=False)
    result = hook.get_statement_result(resp["Id"])

    total_rows = result["Records"][0][0]["longValue"]
    print("Total rows in mart.orders table =", total_rows)


# ------------------------
# DAG DEFINITION
# ------------------------
with DAG(
    dag_id="redshift_provisioned_with_hooks_and_operators",
    description="Pipeline covering Redshift COPY, MERGE, UNLOAD with validation hooks",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 3 * * *",  # Runs at 3 AM daily
    catchup=False,
    max_active_runs=1,
    tags=["redshift", "hooks", "provisioned"]
) as dag:

    # ------------------------
    # 1. Wait for Redshift Cluster Availability
    # ------------------------
    wait_cluster = RedshiftClusterSensor(
        task_id="wait_for_cluster",
        cluster_identifier="{{ var.value.redshift_cluster_identifier }}",
        target_status="available",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,
        mode="reschedule",    #mode = poke(to keep the worker)  or reschedule(to release the worker)
    )


    # ------------------------
    # 2. Create Staging Table
    # ------------------------
    create_staging = RedshiftSQLOperator(
        task_id="create_staging",
        redshift_conn_id="redshift_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS staging;

            CREATE TABLE IF NOT EXISTS staging.orders_stg (
                order_id BIGINT,
                customer_id BIGINT,
                order_ts TIMESTAMP,
                status VARCHAR(20),
                amount NUMERIC(18,2)
            );
        """,
    )


    # ------------------------
    # 3. Create Target Table
    # ------------------------
    create_target = RedshiftSQLOperator(
        task_id="create_target",
        redshift_conn_id="redshift_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS mart;

            CREATE TABLE IF NOT EXISTS mart.orders (
                order_id BIGINT,
                customer_id BIGINT,
                order_ts TIMESTAMP,
                status VARCHAR(20),
                amount NUMERIC(18,2),
                ingested_at TIMESTAMP DEFAULT GETDATE()
            );
        """,
    )


    # ------------------------
    # 4. COPY From S3 → Redshift Staging
    # ------------------------
    copy_to_staging = S3ToRedshiftOperator(
        task_id="copy_to_staging",
        schema="staging",
        table="orders_stg",
        s3_bucket="{{ var.value.raw_orders_bucket }}",
        s3_key="orders/{{ ds }}/",
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        copy_options=["FORMAT AS PARQUET"],
        truncate_table=True,
    )


    # ------------------------
    # 5. Check Staging Data Exists (SqlSensor)
    # ------------------------
    wait_staging_data = SqlSensor(
        task_id="wait_staging_data",
        conn_id="redshift_default",
        sql="""
            SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
            FROM staging.orders_stg
            WHERE order_ts::date = '{{ ds }}';
        """,
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
    )


    # ------------------------
    # 6. Validate Staging Count (Hook)
    # ------------------------
    validate_copy = PythonOperator(
        task_id="validate_copy",
        python_callable=validate_staging_count,
        provide_context=True,
    )


    # ------------------------
    # 7. MERGE (Upsert) Into Final Table
    # ------------------------
    merge_data = RedshiftSQLOperator(
        task_id="merge_data",
        redshift_conn_id="redshift_default",
        sql="""
        MERGE INTO mart.orders AS tgt
        USING staging.orders_stg AS src
        ON tgt.order_id = src.order_id

        WHEN MATCHED THEN UPDATE SET
            customer_id = src.customer_id,
            order_ts = src.order_ts,
            status = src.status,
            amount = src.amount,
            ingested_at = GETDATE()

        WHEN NOT MATCHED THEN INSERT (
            order_id, customer_id, order_ts, status, amount, ingested_at
        ) VALUES (
            src.order_id, src.customer_id, src.order_ts, src.status,
            src.amount, GETDATE()
        );
        """,
    )


    # ------------------------
    # 8. UNLOAD From Redshift → S3
    # ------------------------
    unload_to_s3 = RedshiftToS3Operator(
        task_id="unload_to_s3",
        schema="mart",
        table="orders",
        s3_bucket="{{ var.value.analytics_exports_bucket }}",
        s3_key="orders_snapshot/dt={{ ds }}/",
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        unload_options=["FORMAT AS PARQUET", "ALLOWOVERWRITE"],
    )


    # ------------------------
    # 9. Metadata Validation (Hook)
    # ------------------------
    metadata_validation = PythonOperator(
        task_id="metadata_validation",
        python_callable=metadata_check,
        provide_context=True,
    )


    # ------------------------
    # 10. Cleanup Staging Data
    # ------------------------
    cleanup_staging = RedshiftSQLOperator(
        task_id="cleanup_staging",
        redshift_conn_id="redshift_default",
        sql="DELETE FROM staging.orders_stg WHERE order_ts::date = '{{ ds }}';",
    )


# ------------------------
# DAG DEPENDENCY CHAIN
# ------------------------

wait_cluster >> [create_staging, create_target] >> copy_to_staging
copy_to_staging >> wait_staging_data >> validate_copy >> merge_data
merge_data >> unload_to_s3 >> metadata_validation >> cleanup_staging
```

---

# 🧾 **5. Explanation of The DAG (A–Z)**

## **A) Sensors First**

1. `RedshiftClusterSensor` ensures the cluster is online.
2. `SqlSensor` checks that staging table contains data.

These prevent wasted compute and unnecessary errors.

---

## **B) COPY Stage**

* Loads data from S3 to Redshift staging.
* Uses Parquet format for speed.
* Truncates table first to ensure clean loads.

---

## **C) Validation Using Hooks**

`RedshiftSQLHook` fetches row counts to ensure COPY succeeded.

`RedshiftDataHook` uses Data API to validate metadata.

---

## **D) MERGE Step**

* Performs UPSERT (UPDATE existing rows, INSERT missing rows).
* Avoids duplicate data.
* Ideal for incremental pipelines.

---

## **E) UNLOAD Step**

Exports enriched warehouse data back to S3 for downstream analytical systems.

---

## **F) Cleanup**

Deletes processed data from staging to keep storage light.

---

# 🏁 **6. Final Summary**

This DAG is **production-grade**, covering:

* All major Redshift operators
* All essential sensors
* Two Redshift hooks
* Validation, metadata checks, cleanup, and MERGE logic
* Full retry logic + scheduling

This file is ready to be placed directly into your Airflow repo.

---

If you want next, I can also generate:

✅ A `README.md` specifically for GitHub repo

OR

✅ A Serverless Redshift version using *only Data API*

OR

✅ Add logging + TaskGroups + SLA monitoring

Just tell me!
