# 📘 ETL Pipeline Notes — Spark Declarative Pipelines

> Study notes for the **DLT-PROJECT** pipeline · catalog `learning` · schema `dlt_schema`
> Code lives in `Databricks_ETL/DLT-PROJECT/transformations/` — `bronse.py` (bronze) · `Silver.py` (silver) · `Gold.py` (gold)

---

## 1 · Benefits of Declarative Pipelines

### 1.1 Expectations — data-quality rules

An **expectation** = a rule: a SQL boolean condition on rows, e.g. *"a source column should NOT be null."*

| Action | What happens on violation |
|---|---|
| `expect` | ✅ Row is **kept** — violation only logged in flow metrics |
| `expect_or_drop` | ⬇️ Row is **dropped** from processing |
| `expect_or_fail` | 💥 **Fails the complete pipeline update** |

> ⚠️ **Quarantine is NOT a built-in expectation action.** Storing bad rows separately is something we build ourselves — a separate table that keeps the rows *failing* the check.

### 1.2 Incremental load — handled automatically

* **Streaming tables + checkpoints** → incremental processing out of the box.
* **Auto Loader** → the same for files.
* **Auto CDC flow** → *only* for change data capture: **upserts, deletes, SCD 1 / SCD 2**.

### 1.3 Automatic DAG

> 💡 Databricks derives the **dependency graph (DAG)** from the code — we never wire dependencies manually.

---

## 2 · Backing-Table Storage Anatomy

*What a pipeline-created streaming table looks like under the hood:*

```text
tables/<uuid>/                        ← root of one managed Delta table (the "backing table" the pipeline materializes)
├─ _delta_log/                        ← Delta transaction log = source of truth for the table
│   ├─ 00000000000000000000.json      ← version 0: table created here (CREATE TABLE / first write)
│   ├─ 00000000000000000001.json      ← version 1: first batch of rows appended (one JSON per committed version)
│   └─ ...00000000000010.checkpoint.parquet
│                                     ← every 10 versions the log state is aggregated into parquet,
│                                       so readers don't replay all the JSONs
├─ part-00000-....snappy.parquet      ← the actual row data (parquet "chunks", one file per task/partition batch)
├─ _change_data/                      ← Change Data Feed files — only present when CDF is enabled
│                                       (delta.enableChangeDataFeed); stores row-level INSERT/UPDATE/DELETE
│                                       changes so downstream can consume them incrementally
└─ _dlt_metadata/
   └─ checkpoints/                    ← the PIPELINE's streaming checkpoints (NOT the Delta log checkpoints!)
      └─ ...mmd_user_document_chunks/ ← one folder per flow/table; tracks which source data was already processed
          ├─ 0/, 1/, 2/               ← versioned checkpoint dirs: a HIGHER number = the flow was reset
          │                              (full refresh / recovery) and started a NEW checkpoint lineage;
          │                              only the highest number is current
          ├─ metadata                 ← streaming execution metadata
          ├─ offsets/ 0,1,2...        ← how far the source was read (which files/rows were ingested)
          ├─ commits/ 0,1,2...        ← which batch/offset was successfully committed to the table
          └─ __tmp_path_dir/          ← internal scratch space used during checkpoint writes
```

> ##### 🔑 Key takeaways
>
> * `_delta_log` = the table's **transaction log** (versions; checkpoint file every 10 commits) ·
>   `_dlt_metadata/checkpoints` = the **streaming progress tracker**. Similar names, completely different things.
> * Deleting the `_dlt_metadata` checkpoints (or a **full refresh**) makes the flow **re-read the source from scratch** — the Delta table data still exists until the refresh drops/replaces it.
> * The numbered checkpoint dirs (`0/`, `1/`, `2/`) are the **history of resets** — the current lineage is always the **highest number**.

---

## 3 · 🛡️ Protecting Tables from Accidental Resets in Production

```sql
TBLPROPERTIES ('pipelines.reset.allowed' = false)
```

* Blocks **full refreshes** (including selective full refreshes) on that table → prevents accidental truncation of the table and loss of its streaming checkpoints during pipeline resets.
* Set it as a table property on pipeline-created streaming tables — in the `@dp.table(...)` decorator or `CREATE STREAMING TABLE ... TBLPROPERTIES` — **once a table holds production data**.
* Without it, anyone triggering a full refresh re-reads the whole source from scratch and **wipes the existing checkpoint lineage** (a reset starts a new numbered checkpoint dir — see §2).

> ✅ **Rule of thumb:** production table ⇒ `pipelines.reset.allowed = false`.

---

## 4 · AUTO CDC: INTO (change events) vs FROM SNAPSHOT (full copies)

|  | AUTO CDC **INTO** (change events) | AUTO CDC **FROM SNAPSHOT** (full copies) |
|---|---|---|
| What one file contains | Only new, changed or deleted rows | Every row of the table as of that moment |
| Row count per file | Small, varies with activity | ≈ size of the whole table, every time |
| Typical columns | `op` / `operation` (I/U/D), `updated_at`, LSN or version | Business columns only, maybe an export date |
| How deletes arrive | Explicit delete record (`op = 'D'`) | The row is simply **missing** from the next file |
| Ordering | `SEQUENCE BY` a column in each row | `WITH VERSION` from file name, file time or export date |
| Source system | Debezium, database CDC log, incremental extract | Nightly/hourly full dump, "export the whole table" job |
| Bitemporal support | ✅ Yes (Beta) | ❌ No (SCD Type 1 and 2 only) |
| Cost | Processes only the changes → **cheaper** | Compares a full snapshot each time → **heavier** |

### 🔎 Quick test — which one is my source?

1. **Pick one key** (e.g. `customer_id = 123`) — does it appear in *every* file?
   * Yes, even when nothing changed → the files are **snapshots**
   * Only when that customer changed → they are **change events**
2. **Delete a row** in the source and look at the next file:
   * You see a delete record → use **AUTO CDC INTO**
   * The row is simply gone → use **FROM SNAPSHOT**

### 🧭 Recommendation

* **AUTO CDC INTO** → the default when the source sends incremental changes: cheaper, lower latency, and supports deletes, partial updates and bitemporal history.
* **AUTO CDC FROM SNAPSHOT** → use only when the source can give you nothing but full dumps — the *only* way to detect deletes that aren't sent as records.
* Incremental files with **only upserts and no op column** → still **AUTO CDC INTO** with `KEYS` + `SEQUENCE BY`; you just won't capture deletes.

---

## 5 · Adding AUTO CDC INTO for One Table — Checks First

### ✅ Pre-flight checklist

| Check | What to verify |
|---|---|
| **Pipeline setup** | Serverless, or **Pro / Advanced** edition — *Core doesn't support AUTO CDC* |
| **Source is append-only** | Read it with `STREAM(bronze_table)`; the source must never receive updates/deletes itself — so it can't be another AUTO CDC target |
| **Key columns** | Column(s) that uniquely identify a record (e.g. `customer_id`) — must be **never NULL** |
| **Ordering column** | Reliably orders changes per key (`updated_at`, a version, LSN). Check: never NULL · ties for the same key are rare — else `SEQUENCE BY STRUCT(updated_at, some_id)` · no such column in the CSVs? use `_metadata.file_modification_time` captured in bronze |
| **Delete marker** | Is there an `op` / `operation` column, and what are its values (`D`, `DELETE`, …)? → gives the exact `APPLY AS DELETE WHEN` condition |
| **Columns to leave out** | e.g. `op`, `_rescued_data`, file metadata → `COLUMNS * EXCEPT (...)` |
| **SCD type** | **Type 1** = keeps only the latest row per key · **Type 2** = full history with `__START_AT` / `__END_AT` |
| **Partial rows** | Change record carries only the columns that changed → `COLUMNS TO UPDATE`; a blanket `IGNORE NULL UPDATES` can't apply NULLs you actually want |
| **Target name** | Use a **new** streaming table name — you can't convert an existing streaming table in place, and downstream tables must point to the new one |

### 🧩 The three pieces of code

```python
# 1) Cleaned + validated source — a temporary view (filtering + expectations live here)
# 2) The empty target:
dp.create_streaming_table(name="<target>")
# 3) The flow that wires them together:
dp.create_auto_cdc_flow(
    target="<target>",
    source="<cleaned_view>",
    keys=["<key_column>"],
    sequence_by="<ordering_column>",
    stored_as_scd_type=1,   # 1 = latest only · 2 = full history
)
```

---

## 6 · AUTO CDC — What Can the Flow Write Into?

| Target | Works? | What to do instead |
|---|:---:|---|
| A **new** streaming table declared with `create_streaming_table` | ✅ | This is the standard approach |
| A materialized view | ❌ | Use a streaming table |
| An existing streaming table defined with `@dp.table` (it has its own query) | ❌ | Declare a **new** table name and point downstream tables to it |
| A regular Delta table created outside the pipeline | ❌ | Use a **ForEachBatch sink with MERGE** (Python only) |
| A table owned by another pipeline | ❌ | Write to a table **this** pipeline owns, or update it from the owning pipeline |