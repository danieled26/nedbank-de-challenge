import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipeline.common import (
    create_spark,
    get_or_create_run_state,
    load_config,
    load_dq_rules,
    save_run_state,
    table_path,
    write_delta,
)


def _get_path(config: dict, section: str, key: str, fallback: str) -> str:
    return config.get(section, {}).get(key, fallback)


def stable_sk(col_name: str):
    return F.abs(F.xxhash64(F.col(col_name))).cast("bigint")


def age_band_expr(dob_col: str):
    age = F.floor(F.datediff(F.current_date(), F.col(dob_col)) / F.lit(365.25))
    return (
        F.when(age >= 65, F.lit("65+"))
         .when(age >= 56, F.lit("56-65"))
         .when(age >= 46, F.lit("46-55"))
         .when(age >= 36, F.lit("36-45"))
         .when(age >= 26, F.lit("26-35"))
         .when(age >= 18, F.lit("18-25"))
         .otherwise(F.lit(None).cast("string"))
    )


def write_dq_report(config, fact_count, account_count, customer_count):
    state = get_or_create_run_state()
    dq_rules = load_dq_rules()

    dq_report_path = _get_path(config, "output", "dq_report_path", "/data/output/dq_report.json")

    source_counts = state.get("source_record_counts", {})
    dq_metrics = state.get("dq_metrics", {})

    denominator_map = {
        "duplicate_transactions": source_counts.get("transactions_raw", 0),
        "orphaned_transactions": source_counts.get("transactions_raw", 0),
        "amount_type_mismatch": source_counts.get("transactions_raw", 0),
        "date_format_inconsistency": max(
            source_counts.get("transactions_raw", 0),
            source_counts.get("accounts_raw", 0),
            source_counts.get("customers_raw", 0),
        ),
        "currency_variants": source_counts.get("transactions_raw", 0),
        "null_account_id": source_counts.get("accounts_raw", 0),
    }

    issues = []
    for issue_type, metric in dq_metrics.items():
        records_affected = int(metric.get("records_affected", 0))
        if records_affected <= 0:
            continue

        denom = denominator_map.get(issue_type, 0) or 1
        rule = dq_rules.get(issue_type, {})
        handling_action = rule.get("handling_action", "UNKNOWN")

        issues.append({
            "issue_type": issue_type,
            "records_affected": records_affected,
            "percentage_of_total": round(records_affected * 100.0 / denom, 2),
            "handling_action": handling_action,
            "records_in_output": int(metric.get("records_in_output", 0)),
        })

    start_ts = datetime.strptime(state["run_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    duration = int((datetime.now(timezone.utc) - start_ts).total_seconds())

    total_records = int(source_counts.get("transactions_raw", 0))
    flagged_records = int(sum(i["records_affected"] for i in issues))
    clean_records = max(total_records - flagged_records, 0)
    flag_counts = {i["issue_type"]: i["records_affected"] for i in issues}

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": state["run_timestamp"],
        "stage": str(config.get("runtime", {}).get("stage", "2")),

        # Official Stage 2 report schema
        "source_record_counts": {
            "accounts_raw": int(source_counts.get("accounts_raw", 0)),
            "transactions_raw": int(source_counts.get("transactions_raw", 0)),
            "customers_raw": int(source_counts.get("customers_raw", 0)),
        },
        "dq_issues": issues,
        "gold_layer_record_counts": {
            "fact_transactions": int(fact_count),
            "dim_accounts": int(account_count),
            "dim_customers": int(customer_count),
        },
        "execution_duration_seconds": duration,

        # Backward-compatible fields for the older local harness
        "total_records": total_records,
        "clean_records": clean_records,
        "flagged_records": flagged_records,
        "flag_counts": flag_counts,
    }

    os.makedirs(os.path.dirname(dq_report_path), exist_ok=True)
    with open(dq_report_path, "w") as f:
        json.dump(report, f, indent=2)


def run_provisioning():
    print("STARTING GOLD PROVISIONING", flush=True)

    config = load_config()
    spark = create_spark()

    silver_root = _get_path(config, "output", "silver_path", "/data/output/silver")
    gold_root = _get_path(config, "output", "gold_path", "/data/output/gold")

    customers_s = spark.read.format("delta").load(table_path(silver_root, "customers"))
    accounts_s = spark.read.format("delta").load(table_path(silver_root, "accounts"))
    tx_s = spark.read.format("delta").load(table_path(silver_root, "transactions"))

    dim_customers = (
        customers_s
        .withColumn("customer_sk", stable_sk("customer_id"))
        .withColumn("age_band", age_band_expr("dob"))
        .select(
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        )
    )

    dim_accounts = (
        accounts_s
        .withColumn("account_sk", stable_sk("account_id"))
        .withColumnRenamed("customer_ref", "customer_id")
        .select(
            "account_sk",
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            F.col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
            F.col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
            "last_activity_date",
        )
    )

    account_keys = dim_accounts.select("account_sk", "account_id", "customer_id")
    customer_keys = dim_customers.select("customer_sk", "customer_id")

    fact_transactions = (
        tx_s
        .join(account_keys, on="account_id", how="inner")
        .join(customer_keys, on="customer_id", how="inner")
        .withColumn("transaction_sk", stable_sk("transaction_id"))
        .select(
            "transaction_sk",
            "transaction_id",
            "account_sk",
            "customer_sk",
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            F.col("amount").cast(DecimalType(18, 2)).alias("amount"),
            "currency",
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
        )
    )

    print(f"Writing dim_customers to {table_path(gold_root, 'dim_customers')}", flush=True)
    write_delta(dim_customers, table_path(gold_root, "dim_customers"))

    print(f"Writing dim_accounts to {table_path(gold_root, 'dim_accounts')}", flush=True)
    write_delta(dim_accounts, table_path(gold_root, "dim_accounts"))

    print(f"Writing fact_transactions to {table_path(gold_root, 'fact_transactions')}", flush=True)
    write_delta(fact_transactions, table_path(gold_root, "fact_transactions"))

    customer_count = dim_customers.count()
    account_count = dim_accounts.count()
    fact_count = fact_transactions.count()

    write_dq_report(config, fact_count, account_count, customer_count)

    print("GOLD PROVISIONING COMPLETE", flush=True)