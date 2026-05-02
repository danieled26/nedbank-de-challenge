from pyspark.sql import Window, functions as F
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.common import (
    create_spark,
    get_or_create_run_state,
    load_config,
    save_run_state,
    table_path,
    write_delta,
)


def _get_path(config: dict, section: str, key: str, fallback: str) -> str:
    return config.get(section, {}).get(key, fallback)


def parse_date_expr(col):
    s = F.col(col).cast("string")
    return F.coalesce(
        F.to_date(s, "yyyy-MM-dd"),
        F.to_date(s, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(s.cast("long"))),
        F.to_date(F.from_unixtime((s.cast("double") / F.lit(1000)).cast("long"))),
    )


def normalise_currency_expr(col):
    s = F.upper(F.trim(F.col(col).cast("string")))
    return F.when(s.isin("ZAR", "R", "RANDS", "710"), F.lit("ZAR")).otherwise(s)


def dedupe_latest(df, key_col: str, order_cols: list[str]):
    window = Window.partitionBy(key_col).orderBy(
        *[F.col(c).desc_nulls_last() for c in order_cols]
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def scalar_count(df) -> int:
    return int(df.count())


def scalar_sum(df, col_name: str) -> int:
    rows = df.agg(F.sum(F.col(col_name)).alias("n")).collect()
    value = rows[0]["n"] if rows else None
    return int(value or 0)


def run_transformation():
    print("STARTING SILVER TRANSFORMATION", flush=True)

    config = load_config()
    state = get_or_create_run_state()
    spark = create_spark()

    bronze_root = _get_path(config, "output", "bronze_path", "/data/output/bronze")
    silver_root = _get_path(config, "output", "silver_path", "/data/output/silver")
    quarantine_root = table_path(silver_root, "quarantine")
    work_root = table_path(silver_root, "_work")

    accounts_b = spark.read.format("delta").load(table_path(bronze_root, "accounts"))
    customers_b = spark.read.format("delta").load(table_path(bronze_root, "customers"))
    tx_b = spark.read.format("delta").load(table_path(bronze_root, "transactions"))

    # -------------------------
    # Customers
    # -------------------------
    customers = (
        customers_b
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
        .withColumn("dob_raw", F.col("dob"))
        .withColumn("dob", parse_date_expr("dob"))
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    )

    customers = dedupe_latest(customers, "customer_id", ["ingestion_timestamp"])

    customers = customers.select(
        "customer_id",
        "id_number",
        "first_name",
        "last_name",
        "dob",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "product_flags",
        "ingestion_timestamp",
        "_source_name",
        "_source_file",
        "_pipeline_run_id",
    )

    # -------------------------
    # Accounts
    # -------------------------
    accounts_with_flags = (
        accounts_b
        .withColumn("account_id", F.trim(F.col("account_id")))
        .withColumn("customer_ref", F.trim(F.col("customer_ref")))
        .withColumn(
            "_is_null_account_id",
            F.col("account_id").isNull() | (F.col("account_id") == ""),
        )
        .withColumn("open_date_raw", F.col("open_date"))
        .withColumn("last_activity_date_raw", F.col("last_activity_date"))
        .withColumn("open_date", parse_date_expr("open_date"))
        .withColumn("last_activity_date", parse_date_expr("last_activity_date"))
        .withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )

    null_accounts_q = accounts_with_flags.filter(F.col("_is_null_account_id"))
    null_account_count = scalar_count(null_accounts_q)

    print(
        f"Writing quarantine null_account_id_accounts to "
        f"{table_path(quarantine_root, 'null_account_id_accounts')}",
        flush=True,
    )
    write_delta(
        null_accounts_q,
        table_path(quarantine_root, "null_account_id_accounts"),
    )

    accounts = accounts_with_flags.filter(~F.col("_is_null_account_id"))
    accounts = dedupe_latest(accounts, "account_id", ["ingestion_timestamp"])

    accounts = accounts.select(
        "account_id",
        "customer_ref",
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "mobile_number",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
        "ingestion_timestamp",
        "_source_name",
        "_source_file",
        "_pipeline_run_id",
    )

    # Write customers/accounts early so the heavy transaction path does not hold
    # their lineage in the same action graph.
    print(f"Writing customers to {table_path(silver_root, 'customers')}", flush=True)
    write_delta(customers, table_path(silver_root, "customers"))

    print(f"Writing accounts to {table_path(silver_root, 'accounts')}", flush=True)
    write_delta(accounts, table_path(silver_root, "accounts"))

    # -------------------------
    # Transactions — slim path
    # -------------------------
    if "merchant_subcategory" not in tx_b.columns:
        tx_b = tx_b.withColumn("merchant_subcategory", F.lit(None).cast("string"))


    transactions_path = _get_path(config, "input", "transactions_path", "/data/input/transactions.jsonl")

    raw_amount_flags = (
        spark.read.text(transactions_path)
        .select(
            F.regexp_extract(
                F.col("value"),
                r'"transaction_id"\s*:\s*"([^"]+)"',
                1,
            ).alias("transaction_id"),
            F.col("value")
            .rlike(r'"amount"\s*:\s*"[+-]?\d+(\.\d+)?"')
            .cast("int")
            .alias("_amount_source_string"),
        )
        .filter(F.col("transaction_id") != "")
        .groupBy("transaction_id")
        .agg(F.max("_amount_source_string").alias("_amount_source_string"))
    )


    # Project early. Do not carry nested metadata/location structs or unused raw
    # columns into the dedupe shuffle.
    tx_slim = (
        tx_b
        .select(
            F.trim(F.col("transaction_id")).alias("transaction_id"),
            F.trim(F.col("account_id")).alias("account_id"),
            F.col("amount").cast("string").alias("amount_raw"),
            F.col("currency").cast("string").alias("currency_raw"),
            F.col("transaction_date").cast("string").alias("transaction_date_raw"),
            F.col("transaction_time").cast("string").alias("transaction_time"),
            F.col("transaction_type").cast("string").alias("transaction_type"),
            F.col("merchant_category").cast("string").alias("merchant_category"),
            F.col("merchant_subcategory").cast("string").alias("merchant_subcategory"),
            F.col("channel").cast("string").alias("channel"),
            F.col("location.province").cast("string").alias("province"),
            "ingestion_timestamp",
            "_source_name",
            "_source_file",
            "_pipeline_run_id",
        )
        .withColumn("transaction_date", parse_date_expr("transaction_date_raw"))
        .withColumn("amount", F.col("amount_raw").cast(DecimalType(18, 2)))
        .withColumn("currency", normalise_currency_expr("currency_raw"))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(F.col("transaction_date"), "yyyy-MM-dd"),
                    F.col("transaction_time"),
                ),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn(
            "_date_format_issue",
            F.col("transaction_date_raw").isNotNull()
            & F.col("transaction_date").isNotNull()
            & (~F.col("transaction_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$")),
        )
        .withColumn(
            "_currency_variant_issue",
            F.col("currency_raw").isNotNull()
            & (F.upper(F.trim(F.col("currency_raw"))) != F.lit("ZAR")),
        )
        .withColumn(
            "_amount_cast_failed",
            F.col("amount_raw").isNotNull() & F.col("amount").isNull(),
        )
        .withColumn(
            "_sort_key",
            F.coalesce(
                F.col("transaction_timestamp"),
                F.to_timestamp(F.lit("2999-12-31 23:59:59")),
            ),
        )
    )

    tx_slim = (
        tx_slim
        .join(raw_amount_flags, "transaction_id", "left")
        .withColumn(
            "_amount_source_string",
            F.coalesce(F.col("_amount_source_string"), F.lit(0)),
        )
        .withColumn(
            "_type_mismatch_issue",
            F.col("_amount_cast_failed") | (F.col("_amount_source_string") == F.lit(1)),
        )
    )

    # Materialise the narrow transaction spine.
  
    # lineage and gives the dedupe step a small stable input.
    tx_slim_path = table_path(work_root, "transactions_slim")
    print(f"Writing slim transaction work table to {tx_slim_path}", flush=True)
    write_delta(tx_slim.repartition(16, "transaction_id"), tx_slim_path)

    tx_slim = spark.read.format("delta").load(tx_slim_path)

    # Count issue categories while the table is narrow and materialised.
    pre_dedupe_issue_counts = {
        "amount_type_mismatch": scalar_count(
            tx_slim.filter(F.col("_type_mismatch_issue"))
        ),
        "date_format_inconsistency": scalar_count(
            tx_slim.filter(F.col("_date_format_issue"))
        ),
        "currency_variants": scalar_count(
            tx_slim.filter(F.col("_currency_variant_issue"))
        ),
    }

    # Dedupe without row_number window. min_by keeps the earliest event by sort key
    # while also giving the group size for DQ reporting.
    payload_cols = [c for c in tx_slim.columns if c != "_sort_key"]

    deduped = (
        tx_slim
        .groupBy("transaction_id")
        .agg(
            F.min_by(
                F.struct(*[F.col(c) for c in payload_cols]),
                F.col("_sort_key"),
            ).alias("r"),
            F.count("*").alias("_duplicate_group_count"),
        )
        .select("r.*", "_duplicate_group_count")
        .withColumn("_duplicate_issue", F.col("_duplicate_group_count") > 1)
    )

    dup_records_affected = scalar_sum(
        deduped
        .filter(F.col("_duplicate_group_count") > 1)
        .withColumn(
            "_duplicate_excess_count",
            F.col("_duplicate_group_count") - F.lit(1),
        ),
        "_duplicate_excess_count",
    )

    # Join only against the account key to minimise memory.
   

    valid_accounts = (
        spark.read.format("delta")
        .load(table_path(silver_root, "accounts"))
        .select("account_id")
        .dropDuplicates()
    )

    tx_joined = (
        deduped
        .join(
            valid_accounts.withColumn("_account_exists", F.lit(True)),
            "account_id",
            "left",
        )
        .withColumn(
            "dq_flag",
            F.when(F.col("_account_exists").isNull(), F.lit("ORPHANED_ACCOUNT"))
            .when(F.col("_duplicate_issue"), F.lit("DUPLICATE_DEDUPED"))
            .when(F.col("_type_mismatch_issue"), F.lit("TYPE_MISMATCH"))
            .when(F.col("_date_format_issue"), F.lit("DATE_FORMAT"))
            .when(F.col("_currency_variant_issue"), F.lit("CURRENCY_VARIANT"))
            .otherwise(F.lit(None).cast("string")),
        )
    )

    orphaned_transactions_q = tx_joined.filter(F.col("_account_exists").isNull())
    orphan_count = scalar_count(orphaned_transactions_q)

    print(
        f"Writing quarantine orphaned_transactions to "
        f"{table_path(quarantine_root, 'orphaned_transactions')}",
        flush=True,
    )
    write_delta(
        orphaned_transactions_q,
        table_path(quarantine_root, "orphaned_transactions"),
    )

    transactions = (
        tx_joined
        .filter(F.col("_account_exists").isNotNull())
        .select(
            "transaction_id",
            "account_id",
            "transaction_date",
            "transaction_timestamp",
            "transaction_time",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
            "_source_name",
            "_source_file",
            "_pipeline_run_id",
        )
    )

    print(f"Writing transactions to {table_path(silver_root, 'transactions')}", flush=True)
    write_delta(transactions, table_path(silver_root, "transactions"))

    # Count flags from materialised Silver output, not from the expensive pre-write lineage.
    transactions_out = spark.read.format("delta").load(table_path(silver_root, "transactions"))

    flag_rows = (
        transactions_out
        .groupBy("dq_flag")
        .count()
        .collect()
    )

    flag_counts = {
        row["dq_flag"]: int(row["count"])
        for row in flag_rows
        if row["dq_flag"] is not None
    }

    state["dq_metrics"] = {
        "duplicate_transactions": {
            "records_affected": int(dup_records_affected),
            "records_in_output": int(flag_counts.get("DUPLICATE_DEDUPED", 0)),
        },
        "orphaned_transactions": {
            "records_affected": int(orphan_count),
            "records_in_output": 0,
        },
        "amount_type_mismatch": {
            "records_affected": int(pre_dedupe_issue_counts["amount_type_mismatch"]),
            "records_in_output": int(flag_counts.get("TYPE_MISMATCH", 0)),
        },
        "date_format_inconsistency": {
            "records_affected": int(pre_dedupe_issue_counts["date_format_inconsistency"]),
            "records_in_output": int(flag_counts.get("DATE_FORMAT", 0)),
        },
        "currency_variants": {
            "records_affected": int(pre_dedupe_issue_counts["currency_variants"]),
            "records_in_output": int(flag_counts.get("CURRENCY_VARIANT", 0)),
        },
        "null_account_id": {
            "records_affected": int(null_account_count),
            "records_in_output": 0,
        },
    }

    save_run_state(state)

    print("SILVER TRANSFORMATION COMPLETE", flush=True)