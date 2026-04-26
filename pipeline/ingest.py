from pipeline.common import (
    add_ingestion_columns,
    create_spark,
    get_or_create_run_state,
    load_config,
    save_run_state,
    table_path,
    write_delta,
)


def _get_path(config: dict, section: str, key: str, fallback: str) -> str:
    return config.get(section, {}).get(key, fallback)


def run_ingestion():
    print("STARTING BRONZE INGESTION", flush=True)

    config = load_config()
    state = get_or_create_run_state()
    spark = create_spark()

    accounts_path = _get_path(config, "input", "accounts_path", "/data/input/accounts.csv")
    customers_path = _get_path(config, "input", "customers_path", "/data/input/customers.csv")
    transactions_path = _get_path(config, "input", "transactions_path", "/data/input/transactions.jsonl")

    bronze_root = _get_path(config, "output", "bronze_path", "/data/output/bronze")

    accounts = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(accounts_path)
    )
    accounts = add_ingestion_columns(accounts, "accounts", accounts_path)

    customers = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(customers_path)
    )
    customers = add_ingestion_columns(customers, "customers", customers_path)

    transactions = (
        spark.read
        .option("mode", "PERMISSIVE")
        .json(transactions_path)
    )
    transactions = add_ingestion_columns(transactions, "transactions", transactions_path)

    state["source_record_counts"] = {
        "accounts_raw": accounts.count(),
        "customers_raw": customers.count(),
        "transactions_raw": transactions.count(),
    }
    save_run_state(state)

    print(f"Writing customers to {table_path(bronze_root, 'customers')}", flush=True)
    write_delta(customers, table_path(bronze_root, "customers"))

    print(f"Writing accounts to {table_path(bronze_root, 'accounts')}", flush=True)
    write_delta(accounts, table_path(bronze_root, "accounts"))

    print(f"Writing transactions to {table_path(bronze_root, 'transactions')}", flush=True)
    write_delta(transactions, table_path(bronze_root, "transactions"))

    print("BRONZE INGESTION COMPLETE", flush=True)