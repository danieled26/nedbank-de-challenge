from pyspark.sql import Window, functions as F
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.common import create_spark, load_config, save_run_state, get_or_create_run_state, table_path, write_delta


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
    window = Window.partitionBy(key_col).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def run_transformation():
    print("STARTING SILVER TRANSFORMATION", flush=True)

    config = load_config()
    state = get_or_create_run_state()
    spark = create_spark()

    bronze_root = _get_path(config, "output", "bronze_path", "/data/output/bronze")
    silver_root = _get_path(config, "output", "silver_path", "/data/output/silver")

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
        .withColumn("_is_null_account_id", F.col("account_id").isNull() | (F.col("account_id") == ""))
        .withColumn("open_date_raw", F.col("open_date"))
        .withColumn("last_activity_date_raw", F.col("last_activity_date"))
        .withColumn("open_date", parse_date_expr("open_date"))
        .withColumn("last_activity_date", parse_date_expr("last_activity_date"))
        .withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )

    null_account_count = accounts_with_flags.filter(F.col("_is_null_account_id")).count()

    accounts = (
        accounts_with_flags
        .filter(~F.col("_is_null_account_id"))
    )
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

    # -------------------------
    # Transactions
    # -------------------------
    has_subcat = "merchant_subcategory" in tx_b.columns
    if not has_subcat:
        tx_b = tx_b.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    tx = (
        tx_b
        .withColumn("transaction_id", F.trim(F.col("transaction_id")))
        .withColumn("account_id", F.trim(F.col("account_id")))
        .withColumn("amount_raw", F.col("amount").cast("string"))
        .withColumn("currency_raw", F.col("currency").cast("string"))
        .withColumn("transaction_date_raw", F.col("transaction_date").cast("string"))
        .withColumn("transaction_date", parse_date_expr("transaction_date"))
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("currency", normalise_currency_expr("currency"))
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.date_format(F.col("transaction_date"), "yyyy-MM-dd"), F.col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("province", F.col("location.province"))
    )

    tx = tx.withColumn(
        "_date_format_issue",
        (F.col("transaction_date").isNotNull()) & (~F.col("transaction_date_raw").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    ).withColumn(
        "_currency_variant_issue",
        F.upper(F.trim(F.col("currency_raw"))) != F.lit("ZAR")
    ).withColumn(
        "_type_mismatch_issue",
        F.col("amount_raw").rlike(r"^['\"].*['\"]$")
    )

    duplicate_ids = (
        tx.groupBy("transaction_id")
          .count()
          .filter(F.col("count") > 1)
          .select("transaction_id")
          .withColumn("_duplicate_issue", F.lit(True))
    )

    tx = tx.join(duplicate_ids, "transaction_id", "left").withColumn(
        "_duplicate_issue", F.coalesce(F.col("_duplicate_issue"), F.lit(False))
    )

    dup_records_affected = duplicate_ids.count()

    tx_window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").asc_nulls_last(),
        F.col("ingestion_timestamp").asc_nulls_last(),
    )

    tx = (
        tx.withColumn("_rn", F.row_number().over(tx_window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    valid_accounts = accounts.select("account_id").dropDuplicates()
    tx = tx.join(valid_accounts.withColumn("_account_exists", F.lit(True)), "account_id", "left")

    orphan_count = tx.filter(F.col("_account_exists").isNull()).count()

    tx = tx.withColumn(
        "dq_flag",
        F.when(F.col("_account_exists").isNull(), F.lit("ORPHANED_ACCOUNT"))
         .when(F.col("_duplicate_issue"), F.lit("DUPLICATE_DEDUPED"))
         .when(F.col("_type_mismatch_issue"), F.lit("TYPE_MISMATCH"))
         .when(F.col("_date_format_issue"), F.lit("DATE_FORMAT"))
         .when(F.col("_currency_variant_issue"), F.lit("CURRENCY_VARIANT"))
         .otherwise(F.lit(None).cast("string"))
    )

    # Exclude orphaned transactions from Silver/Gold to preserve FK integrity.
    tx = tx.filter(F.col("_account_exists").isNotNull())

    transactions = tx.select(
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

    # DQ metrics for report.
    state["dq_metrics"] = {
        "duplicate_transactions": {
            "records_affected": int(dup_records_affected),
            "records_in_output": int(transactions.filter(F.col("dq_flag") == "DUPLICATE_DEDUPED").count()),
        },
        "orphaned_transactions": {
            "records_affected": int(orphan_count),
            "records_in_output": 0,
        },
        "amount_type_mismatch": {
            "records_affected": int(tx.filter(F.col("_type_mismatch_issue")).count()),
            "records_in_output": int(transactions.filter(F.col("dq_flag") == "TYPE_MISMATCH").count()),
        },
        "date_format_inconsistency": {
            "records_affected": int(tx.filter(F.col("_date_format_issue")).count()),
            "records_in_output": int(transactions.filter(F.col("dq_flag") == "DATE_FORMAT").count()),
        },
        "currency_variants": {
            "records_affected": int(tx.filter(F.col("_currency_variant_issue")).count()),
            "records_in_output": int(transactions.filter(F.col("dq_flag") == "CURRENCY_VARIANT").count()),
        },
        "null_account_id": {
            "records_affected": int(null_account_count),
            "records_in_output": 0,
        },
    }
    save_run_state(state)

    print(f"Writing customers to {table_path(silver_root, 'customers')}", flush=True)
    write_delta(customers, table_path(silver_root, "customers"))

    print(f"Writing accounts to {table_path(silver_root, 'accounts')}", flush=True)
    write_delta(accounts, table_path(silver_root, "accounts"))

    print(f"Writing transactions to {table_path(silver_root, 'transactions')}", flush=True)
    write_delta(transactions, table_path(silver_root, "transactions"))

    print("SILVER TRANSFORMATION COMPLETE", flush=True)