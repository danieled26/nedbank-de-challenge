# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** Edwin Daniels  
**Date:** 2026-05-03  
**Status:** Final

---

## Context

Stage 3 added a stream source for the virtual account mobile app. The batch pipeline still had to run first, but the app also needed current balances and recent transactions to update from transaction events. The stream input is a set of JSONL micro-batch files under `/data/stream/`. The files are named in chronological order, for example `stream_20260320_143000_0001.jsonl`. The event schema is the same as the Stage 2 transaction JSON, including `merchant_subcategory`.

The new outputs are two Delta tables under `/data/output/stream_gold/`. `current_balances` has one row per valid `account_id`, with `current_balance`, `last_transaction_timestamp`, and `updated_at`. `recent_transactions` keeps recent transaction events by `account_id` and `transaction_id`, with a maximum of 50 transactions per account. The SLA is checked as `updated_at - transaction_timestamp`, and must be no more than 300 seconds.

Before Stage 3, the code was already split into batch modules: `pipeline/ingest.py`, `pipeline/transform.py`, `pipeline/provision.py`, `pipeline/common.py`, and `pipeline/run_all.py`. Stage 2 had added DQ handling, `dq_report.json`, `dq_rules.yaml`, quarantine outputs, `merchant_subcategory` support, a slim transaction work table at `/data/output/silver/_work/transactions_slim`, and Spark local spill configuration to use `/data/output/_spark_tmp` instead of `/tmp`. Stage 3 added `pipeline/stream_ingest.py`, stream settings in `pipeline_config.yaml`, and one call to `run_stream_ingestion()` in `run_all.py` after batch provisioning. Final code line counts were: `common.py` 130 lines, `ingest.py` 67 lines, `transform.py` 426 lines, `provision.py` 214 lines, `stream_ingest.py` 230 lines, and `run_all.py` 26 lines.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

The Stage 1 structure helped because the batch work was already separated by pipeline stage. `ingest.py` writes Bronze, `transform.py` writes Silver, `provision.py` writes Gold, and `common.py` contains shared Spark, config, path, run-state, and Delta write helpers. Because of that, Stage 3 did not require a rewrite of the batch path. I added `stream_ingest.py` as a separate module and called it from `run_all.py` after `run_provisioning()`. The config pattern also helped. The pipeline was already using `/data/input`, `/data/config`, and `/data/output`, so adding `/data/stream` and `/data/output/stream_gold` fit the existing approach.

Stage 2 also helped because the transaction path had already been hardened. `transform.py` already dealt with mixed date formats, amount casting, currency normalisation, `merchant_subcategory`, duplicate transactions, orphaned transactions, and account filtering. The stream input used the same transaction shape, so `stream_ingest.py` followed the same basic rules: build `transaction_timestamp`, cast `amount` to `DECIMAL(18,2)`, normalise currency to `ZAR`, deduplicate by `transaction_id`, and keep only events for accounts in batch `dim_accounts`.

The main weakness was that some useful logic was not reusable enough. `transform.py` became large during Stage 2 and contains parsing, DQ checks, deduplication, quarantine handling, and metrics in one file. `stream_ingest.py` repeats some date and currency logic because there is no shared `transactions.py` helper module. `run_all.py` is also very simple. It works for the challenge, but it is not a mode-based orchestrator. It now runs batch and then stream in one sequence. About 80–85% of the Stage 1/2 code survived into Stage 3. The Stage 3 work was mainly additive: one new stream module, a small config update, and one extra call in the entry point.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

I would move common transaction logic into a shared module. Right now `transform.py` and `stream_ingest.py` both contain similar logic for parsing dates, creating timestamps, casting amounts, and normalising currency. That is not ideal. A better structure would be `pipeline/transactions.py` with functions such as `parse_date_expr`, `normalise_currency_expr`, and `build_transaction_timestamp`. Then both batch and stream processing would use the same code.

I would also move output schemas into one place. `provision.py` currently defines the Gold output columns inline. `stream_ingest.py` defines the stream output columns separately. This is clear enough for the current challenge, but it creates a risk of schema drift. A small `pipeline/schemas.py` file would make the expected Gold and stream Gold schemas explicit.

Finally, I would make DQ execution more directly driven by `dq_rules.yaml`. The current config defines the issue names and handling actions, and `provision.py` uses it when writing `dq_report.json`, but the detection logic still lives mostly in `transform.py`. A better design would map each DQ rule to a detector and handler function.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If I had known Stage 3 was coming from the start, I would design the pipeline around a shared transaction event contract. Batch transactions from `/data/input/transactions.jsonl` and stream transactions from `/data/stream/stream_*.jsonl` would both pass through the same normalisation layer. That layer would standardise the schema, parse the date, create `transaction_timestamp`, cast `amount`, normalise currency, and return a common transaction dataframe. The batch and stream paths would then differ only in how they use the cleaned events.

For state, I would still use Delta because the required output format is Delta Parquet and the stream state is small. But I would model state tables from day one. `current_balances` would be an account state table keyed by `account_id`. `recent_transactions` would be a bounded state table keyed by `(account_id, transaction_id)` with a standard last-50 retention function. In this submission the stream files are all available at container start, so the implementation processes them deterministically and writes final state. That is enough for this challenge, but an upfront design would make incremental upsert state the normal path.

---

## Appendix

### Final Stage 3 pipeline shape

/data/input/
  accounts.csv
  customers.csv
  transactions.jsonl
        |
        v
pipeline/ingest.py
  /data/output/bronze/accounts
  /data/output/bronze/customers
  /data/output/bronze/transactions
        |
        v
pipeline/transform.py
  /data/output/silver/accounts
  /data/output/silver/customers
  /data/output/silver/transactions
  /data/output/silver/_work/transactions_slim
  /data/output/silver/quarantine/null_account_id_accounts
  /data/output/silver/quarantine/orphaned_transactions
        |
        v
pipeline/provision.py
  /data/output/gold/dim_accounts
  /data/output/gold/dim_customers
  /data/output/gold/fact_transactions
  /data/output/dq_report.json
        |
        v
pipeline/stream_ingest.py
  reads /data/stream/stream_*.jsonl
  writes /data/output/stream_gold/current_balances
  writes /data/output/stream_gold/recent_transactions