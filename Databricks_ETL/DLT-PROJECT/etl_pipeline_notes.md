Declarative PL - 
Benefits : 
1. expectations- we add a rule (a SQL boolean condition), for example a source column should NOT be null. On violation the actions are: warn only (row is kept, violation logged in metrics), drop the row from processing, or fail the complete PL execution.
   quarantine (storing bad rows separately) is NOT a built-in expectation action - we build it ourselves as a separate table that keeps the rows failing the check.
2.incremental load -
   streaming tables + checkpoints handle the incremental load automatically (Auto Loader does it for files).
   Auto CDC flow is only for change data capture: upserts, deletes, SCD1/SCD2. 
3. databricks can determine the DAG or dependences without we specifying it manually.

4. backing table storage anatomy (what a pipeline-created streaming table looks like under the hood):
   tables/<uuid>/                      <- root folder of one managed Delta table (the "backing table" the pipeline materializes)
   ├─ _delta_log/                      <- Delta transaction log = source of truth for the table.
   │   ├─ 00000000000000000000.json    <- version 0: table was created here (CREATE TABLE / first write).
   │   ├─ 00000000000000000001.json    <- version 1: first batch of rows appended (one JSON per committed version).
   │   └─ 00000000000000000010.checkpoint.parquet <- checkpoint: every 10 versions the log state is aggregated into parquet so readers don't replay all JSONs.
   ├─ part-00000-....snappy.parquet    <- the actual row data, written as parquet "chunks" (one file per task/partition batch).
   ├─ _change_data/                    <- Change Data Feed files; only present when CDF (delta.enableChangeDataFeed) is enabled - stores row-level INSERT/UPDATE/DELETE changes so downstream can consume them incrementally.
   └─ _dlt_metadata/
      └─ checkpoints/                  <- the pipeline's streaming checkpoints (NOT the Delta log checkpoints).
          └─ ...mmd_user_document_chunks/  <- one folder per flow/table; tracks which source data was already processed.
             ├─ 0/, 1/, 2/            <- versioned checkpoint dirs - a higher number means the flow was reset (full refresh/recovery) and started a new checkpoint lineage; only the highest is current.
             ├─ metadata              <- streaming execution metadata.
             ├─ offsets/ 0,1,2...     <- how far the source was read (which files/rows were ingested).
             └─ commits/ 0,1,2...      <- which batch/offset was successfully committed to the table.
             └─ __tmp_path_dir/        <- internal scratch space used during checkpoint writes.
   key takeaways:
   - _delta_log is the table's transaction log (versions, checkpoints every 10 commits); _dlt_metadata/checkpoints is the streaming progress tracker - they are different things with similar names.
   - if we delete the _dlt_metadata checkpoints (or do a full refresh), the flow re-reads the source from scratch, but the Delta table data still exists until the refresh drops/replaces it.
   - the numbered checkpoint dirs (0/, 1/, 2/) show the history of resets - the current lineage is always the highest number.

5. protecting tables from accidental resets in production:
   table property "pipelines.reset.allowed" = false prevents full table refreshes - a full refresh (or selective full refresh) on that table is blocked, which helps avoid accidental truncation of the table and loss of its streaming checkpoints during pipeline resets.
   - set it as a table property on pipeline-created streaming tables (e.g. in the @dlt.table(...) decorator / CREATE STREAMING TABLE ... TBLPROPERTIES) once a table holds production data.
   - without it, anyone triggering a full refresh re-reads the whole source from scratch and wipes the existing checkpoint lineage (see point 4: a reset starts a new numbered checkpoint dir).

6. AUTO CDC INTO (change events) vs AUTO CDC FROM SNAPSHOT (full copies):

| | AUTO CDC INTO (change events) | AUTO CDC FROM SNAPSHOT (full copies) |
| --- | --- | --- |
| What one file contains | Only new, changed or deleted rows | Every row of the table as of that moment |
| Row count per file | Small, and varies with activity | About the size of the whole table every time |
| Typical columns | op/operation (I/U/D), updated_at, LSN or version | Business columns only, maybe an export date |
| How deletes arrive | As an explicit delete record (op = 'D') | The row is missing from the next file |
| Ordering | SEQUENCE BY a column in each row | WITH VERSION from file name, file time or export date |
| Source system | Debezium, database CDC log, incremental extract | Nightly or hourly full dump, "export the whole table" job |
| Bitemporal support | Yes (Beta) | No (SCD Type 1 and 2 only) |
| Cost | Processes only the changes, so cheaper | Compares a full snapshot each time, so heavier |

   quick test - identify which one the source is:
   - pick one key, for example customer_id = 123, and check whether it appears in every file. If it does, even when nothing changed, the files are snapshots. If it appears only when that customer changed, they are change events.
   - delete a row in the source and look at the next file. If you see a delete record, use AUTO CDC INTO. If the row is simply gone, use FROM SNAPSHOT.

   recommendation:
   - AUTO CDC INTO: the default when the source sends incremental changes. It is cheaper, has lower latency, and supports deletes, partial updates and bitemporal history.
   - AUTO CDC FROM SNAPSHOT: use it only when the source can give you nothing but full dumps. It is the only way to detect deletes that aren't sent as records.
   - incremental files that contain only upserts, with no op column: still use AUTO CDC INTO with KEYS and SEQUENCE BY. You just won't capture deletes.

7. adding AUTO CDC INTO for one table - a few checks first, then three pieces of code:

   what to check first:
   - pipeline setup: the pipeline must be serverless, or on the Pro or Advanced edition. Core edition doesn't support AUTO CDC.
   - source is append-only: read it with STREAM(bronze_table). The source must not itself get updates or deletes, so it can't be another AUTO CDC target.
   - key columns: pick the column(s) that uniquely identify a record, such as customer_id. Make sure they are never NULL.
   - ordering column: you need a column that reliably orders changes per key, such as updated_at, a version or LSN. Check:
     * it is never NULL.
     * ties for the same key are rare. If they happen, use SEQUENCE BY STRUCT(updated_at, some_id).
     * if the CSVs have no such column, use _metadata.file_modification_time captured in bronze.
   - delete marker: check whether there is an op/operation column and what its values are (D, DELETE, ...). That gives you the exact APPLY AS DELETE WHEN condition.
   - columns to leave out: for example op, _rescued_data, file metadata. Leave them out with COLUMNS * EXCEPT (...).
   - SCD type: Type 1 keeps only the latest row per key. Type 2 keeps full history with __START_AT/__END_AT.
   - partial rows: if a change record carries only the columns that changed, use COLUMNS TO UPDATE. A blanket IGNORE NULL UPDATES can't apply NULLs you actually want.
   - target name: use a new streaming table name. You can't convert an existing streaming table in place, and downstream tables must point to the new one.

8. AUTO CDC - what kind of target can the flow write into:

| Target | Works? | What to do instead |
| --- | --- | --- |
| A new streaming table declared with create_streaming_table | Yes | This is the standard approach |
| A materialized view | No | Use a streaming table |
| An existing streaming table defined with @dp.table (it has its own query) | No | Declare a new table name and point downstream tables to it |
| A regular Delta table created outside the pipeline | No | Use a ForEachBatch sink with MERGE (Python only) |
| A table owned by another pipeline | No | Write to a table this pipeline owns, or update it from the owning pipeline |