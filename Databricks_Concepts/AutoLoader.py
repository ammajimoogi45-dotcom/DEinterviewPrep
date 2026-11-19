# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ##AUTO LOADER 
# MAGIC - is used for loading the data from the cloud storage incrementally when the files arrived in the storage directory.
# MAGIC - we can process the existing files in the directory
# MAGIC - checkpointLocation is used inside the writeStream
# MAGIC - schemaLocation is used inside the readStream
# MAGIC - streaming data can't be seen with show()/collect() - the stream must be executed with a sink:
# MAGIC   - quick preview: display(df, checkpointLocation=...) - internal memory sink, live table in the notebook (max 10,000 rows)
# MAGIC   - real sink example: df.writeStream.format("console").option("checkpointLocation", ...).trigger('availableNow').start() -> query.awaitTermination(30) -> query.stop()
# MAGIC     - trigger('availableNow') = process all data available right now once, then stop
# MAGIC     - console sink prints the rows to the cell output
# MAGIC     - awaitTermination() waits until the query finishes; stop() releases it
# MAGIC - checkpoint folder anatomy (what each file does):
# MAGIC   - metadata - the query's unique id; a restarted query must match it or recovery is refused
# MAGIC   - offsets/N - what the query PLANNED to process: the source's latest offset (which files were discovered), watermark, configs; first line is the format version "v1"
# MAGIC   - commits/N - batches actually COMMITTED to the sink; a batch in offsets but missing in commits is replayed on restart (this is how exactly-once works)
# MAGIC   - sources/0/rocksdb/ - Auto Loader's file-listing state (every file name already seen) - this is what makes the next run skip existing files (the incremental part)
# MAGIC   - state/0/ - state store for stateful operators (e.g. a streaming limit)
# MAGIC   - read them with dbutils.fs.ls(path) and dbutils.fs.head(path, n), or %fs ls / %fs head - text files are JSON with a "v1" version line, never edit them
# MAGIC - never place the checkpoint (or schema) folder inside the source directory - Auto Loader lists the source recursively and ingests those files as data (that is where the "v1" column in the UNKNOWN_FIELD error came from)
# MAGIC - 

# COMMAND ----------

# DBTITLE 1,How Auto Loader identifies new files
# MAGIC %md
# MAGIC ## How Auto Loader identifies the new files
# MAGIC
# MAGIC Auto Loader has **three file discovery modes** – it finds new files, compares them against the file-listing state saved in the checkpoint, and processes only the ones it has never seen before.
# MAGIC
# MAGIC ### 1. Directory listing (default – no option needed)
# MAGIC - Auto Loader lists the files in the source folder and compares them with the file-listing state in the checkpoint (the `sources/0/rocksdb/` folder).
# MAGIC - First run = full directory listing; later runs can use **incremental listing** on supported cloud storage (lists only new directories).
# MAGIC - Simple to use – no cloud permissions or setup – but for large directories listing is slow.
# MAGIC
# MAGIC ### 2. File notification mode (classic) – `cloudFiles.useNotifications = true`
# MAGIC - Cloud storage events track file uploads (S3 → SNS + SQS, ADLS/Blob → Event Grid + Queue Storage, GCS → Pub/Sub) instead of listing the directory.
# MAGIC - **One dedicated queue is created per stream**; once a file name is read from the queue, it is removed from that queue (Auto Loader needs message-delete permission for this).
# MAGIC - Requires **elevated cloud permissions** so Auto Loader can create the notification/queue resources, and the source path cannot be changed later.
# MAGIC - Cloud providers limit the number of notification pipelines (100 per S3/GCS bucket, 500 per Azure storage account) – many streams on one bucket can hit these limits.
# MAGIC
# MAGIC ### 3. Managed file events (recommended) – `cloudFiles.useManagedFileEvents = true`
# MAGIC - The queue is **shared among all streams** on the external location; each stream maintains its own read position in its checkpoint, saying up to where it has read from the queue.
# MAGIC - Databricks **sets up and manages the cloud resources automatically** – fewer credentials and IAM policies than classic notification mode.
# MAGIC - **First run always does a full directory listing** to sync with the file events cache; subsequent runs discover files incrementally from the cache using the stored read position.
# MAGIC - Old cache entries are removed after a while – the stored read position stays valid for only **7 days**, which is why the docs say to run the stream at least once every 7 days. After that the position is invalid and Auto Loader falls back to a full directory listing.
# MAGIC
# MAGIC #### Prerequisites for managed file events
# MAGIC
# MAGIC **Databricks requirements**
# MAGIC - Workspace must have **Unity Catalog** enabled.
# MAGIC - **Databricks Runtime 14.3 LTS or above**.
# MAGIC - Permission to create: **Storage Credential** and **External Location** objects in Unity Catalog.
# MAGIC
# MAGIC **Unity Catalog setup**
# MAGIC 1. **Storage Credential** – the identity Databricks uses to access cloud storage (for Azure: an Access Connector for Azure Databricks).
# MAGIC 2. **External Location** – maps a cloud storage path to the storage credential; Auto Loader must read from this external location.
# MAGIC 3. **Enable File Events** on the external location (Catalog Explorer → **Automatic setup**, recommended).
# MAGIC
# MAGIC **Azure permissions for automatic setup (ADLS Gen2)**
# MAGIC
# MAGIC | Azure role | Purpose | Assigned at |
# MAGIC | --- | --- | --- |
# MAGIC | Storage Blob Data Contributor | Read/write access to storage data | Storage account |
# MAGIC | Storage Queue Data Contributor | Access to file event notification queues | Storage account |
# MAGIC | Storage Account Contributor | Lets Databricks configure file events automatically | Storage account |
# MAGIC | EventGrid EventSubscription Contributor | Lets Databricks create Event Grid subscriptions | Resource group containing the storage account |
# MAGIC
# MAGIC **Do these apply to classic `useNotifications`? No.**
# MAGIC - Classic notification mode does **not** need UC external locations or file events – its prerequisite is different: **elevated cloud permissions** (or a service credential) so Auto Loader itself can create the per-stream notification service and queue in your cloud account.
# MAGIC
# MAGIC > **Interview takeaway:** listing = no setup but slow; classic notifications = fast but one queue per stream + cloud admin permissions; managed file events = shared queue, auto-managed by Databricks, and the currently recommended mode for production.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Micro-batch execution lifecycle (what happens inside the stream)
# MAGIC
# MAGIC **Step 1 – Discover.** Using one of the three modes above, the source finds the new files and reports the latest available offset to the engine.
# MAGIC
# MAGIC **Step 2 – Plan the batch.** The query writes the planned offset range to `offsets/N` in the checkpoint.
# MAGIC - Default / `processingTime` trigger: exactly **one batch is planned per trigger** – nothing is fixed in advance.
# MAGIC - `availableNow` trigger: the **full set of pending batches is determined up front** (sized by `maxFilesPerTrigger` / `maxBytesPerTrigger`), including any leftover uncommitted batches from a previous run.
# MAGIC
# MAGIC **Step 3 – Execute.** Read the new files in the offset range → parse (apply schema) → transform → **write to the sink**. At this point the batch is **written but not yet committed** – not visible to the user / not recorded as done.
# MAGIC
# MAGIC **Step 4 – Commit.** The commit is recorded in `commits/N`. Now the batch is final and its output is visible. If the job crashes between Step 3 and Step 4, the batch exists in `offsets` but not in `commits`, so it is **replayed on restart** – this is how exactly-once works.
# MAGIC
# MAGIC The loop then continues for the next batch (for `availableNow`: one after the other until all determined batches are done, then the query stops).
# MAGIC
# MAGIC ### Trigger semantics
# MAGIC
# MAGIC | Trigger | Behavior |
# MAGIC | --- | --- |
# MAGIC | default (no trigger) | Runs micro-batches back-to-back – as soon as one batch finishes, it immediately checks for new data. |
# MAGIC | `processingTime='1 minute'` | Starts a new batch every 1 minute. The interval is counted from when the **previous batch finishes**, not wall-clock time – batches never overlap, so a slow batch delays the next one. |
# MAGIC | `availableNow=True` | Takes a snapshot of all available data at start, processes it in possibly **multiple batches one after the other**, then stops on its own. Best for incremental/scheduled loads. |
# MAGIC ### Available trigger types
# MAGIC
# MAGIC | Trigger | Syntax | Behaviour |
# MAGIC |---|---|---|
# MAGIC | ProcessingTime | `.trigger(processingTime='1 minute')` | Processes data repeatedly at the specified interval |
# MAGIC | AvailableNow | `.trigger(availableNow=True)` | Processes all currently available data and stops |
# MAGIC | Once (legacy) | `.trigger(once=True)` | Processes available data in one batch and stops |
# MAGIC | Default | omit `.trigger(...)` | Continuously processes data as it arrives (micro-batches) |
# MAGIC
# MAGIC > note: on serverless compute only AvailableNow / Once work - the Default (continuous) mode and ProcessingTime throw `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` (hit this in the toTable cell). use a classic cluster for continuous streaming.
# MAGIC
# MAGIC
# MAGIC > **Interview takeaway:** the checkpoint pair `offsets/N` (planned) + `commits/N` (done) is the heart of exactly-once; the trigger only decides **when** the next micro-batch starts, never how commits work.

# COMMAND ----------

# DBTITLE 1,Batch size
# MAGIC %md
# MAGIC ### Batch size – how it is determined
# MAGIC
# MAGIC - Auto Loader sizes each batch by counting **files and bytes, not rows**.
# MAGIC - Two options control the size – a batch is closed when either limit is reached:
# MAGIC
# MAGIC | Option | Default | Meaning |
# MAGIC | --- | --- | --- |
# MAGIC | `cloudFiles.maxFilesPerTrigger` | `1000` | Maximum number of files in one batch. |
# MAGIC | `cloudFiles.maxBytesPerTrigger` | Not set | Rough maximum size of one batch, e.g. `"10g"`. It is a **soft limit**: a batch always takes at least one file, even if that file alone is bigger than the limit. |
# MAGIC
# MAGIC Usage:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.maxFilesPerTrigger", 500)      # limit files per batch
# MAGIC .option("cloudFiles.maxBytesPerTrigger", "5g")     # limit size per batch
# MAGIC ```
# MAGIC
# MAGIC ### Other sources – Kafka / Event Hubs
# MAGIC
# MAGIC - Message-based sources have their own equivalent knob: **`maxOffsetsPerTrigger`** – the maximum number of **messages** (offset ranges) processed per micro-batch.
# MAGIC - Same idea as Auto Loader's file/byte limits, just counted in messages instead of files.
# MAGIC
# MAGIC ### Clean source – remove files after processing
# MAGIC
# MAGIC - `cloudFiles.cleanSource` moves or deletes files from the source directory **after they are processed** (default `OFF`).
# MAGIC - Two modes: `MOVE` (archive to `cloudFiles.cleanSource.moveDestination`) or `DELETE`.
# MAGIC - Only **already processed** files are cleaned – and only safe if **no other stream reads the same folder**.
# MAGIC - `cloudFiles.cleanSource.retentionDuration` (default **30 days**) must elapse before files become cleanup candidates; `DELETE` requires **more than 7 days**.
# MAGIC - MOVE destination must **not** be inside the source directory (archived files would be re-ingested) and must be in the same external location / volume / mount.
# MAGIC - Cleanup is **not a background job** – it runs only when the stream processes a new batch; `cloudFiles.cleanSource.waitForCompletion=true` keeps an `availableNow` run alive until cleanup finishes.
# MAGIC - With `DELETE`, enable bucket versioning so deletes act as soft-deletes (recovery safety).
# MAGIC

# COMMAND ----------

# DBTITLE 1,AutoLoader Reading
df=spark.readStream.format('cloudFiles').option('cloudFiles.format','csv').option('cloudFiles.inferColumnTypes',True).option('header',True).option('cloudFiles.schemaLocation','/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/schema/').load(
    '/Volumes/learning/dlt_schema/rawfilesystem/Landing/order_sample/'
)


print(df.isStreaming)

# COMMAND ----------

# DBTITLE 1,outputs are written to driver log
query = (
    df.writeStream
    .format("console")
    .option("checkpointLocation", "/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/consolecheckpoint/")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination(30)
query.stop()

print("Is active:", query.isActive)
print("Status:", query.status)
print("Progress:", query.lastProgress)
print("Exception:", query.exception())

# COMMAND ----------

# DBTITLE 1,written to memory table
query = (
    df.writeStream
    .format("memory")                     # rows land in an in-memory temp table
    .queryName("orders_preview")          # becomes a queryable table name
    .option("checkpointLocation", "/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/memorycheckpoint/")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination(30)

display(spark.sql("SELECT * FROM orders_preview"))   # now the rows are visible

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state(
# MAGIC   '/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/consolecheckpoint/sources/0')

# COMMAND ----------

dbutils.fs.ls('/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/schema/_schemas/')
dbutils.fs.head('/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/schema/_schemas/0')

# COMMAND ----------

1. how to control schema evolution

3. how to load all the files or do the full load 



# COMMAND ----------

# DBTITLE 1,Schema Inference and Evolution in Auto Loader
# MAGIC %md
# MAGIC ## Schema Inference and Evolution in Auto Loader
# MAGIC
# MAGIC ### Enabling it
# MAGIC - Mention `cloudFiles.schemaLocation` (**readStream**) – required to enable schema inference and evolution; the inferred schema is stored there in `_schemas/0`.
# MAGIC - Mention `checkpointLocation` (**writeStream**) – tracks the source, offsets and commits.
# MAGIC - In Spark Declarative Pipelines, Databricks manages both the schema location and checkpoint for you.
# MAGIC - If you load more than one source data location into the target table, **each Auto Loader ingestion workload needs its own separate streaming checkpoint**.
# MAGIC
# MAGIC ### Sample size for schema inference
# MAGIC Auto Loader infers the schema from a sample of the data. Control the sample with:
# MAGIC > `spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes` – a byte string such as `10gb`
# MAGIC > `spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles` – an integer
# MAGIC
# MAGIC ### What happens when a new column arrives in a file
# MAGIC - The stream fails with `UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE`.
# MAGIC - Default mode if no schema is provided = `addNewColumns`: the new column is added to the schema, so **retry / re-run** resolves the issue.
# MAGIC - Default mode if a schema IS provided = `none`.
# MAGIC
# MAGIC ### `cloudFiles.schemaEvolutionMode` – the modes
# MAGIC
# MAGIC | Mode | Behavior |
# MAGIC | --- | --- |
# MAGIC | `addNewColumns` | Adds the new column to the existing schema. **Default when no schema is provided.** |
# MAGIC | `addNewColumnsWithTypeWidening` | `addNewColumns` **+ widens compatible types**: e.g. `int → long`, `float → double`, higher decimal precision. Unsupported changes (`int → string`) fall back to `_rescued_data`. Requires `mergeSchema=true` on the write and type widening enabled on the target table; the stream still fails once on a type change – restart picks up the evolved schema. |
# MAGIC | `rescue` | Never fails the run. New columns are **not** added to the schema – their values are stored in the `_rescued_data` column. |
# MAGIC | `failOnNewColumns` | Fails the stream on new columns; the schema never evolves (use when the schema must stay fixed). |
# MAGIC | `none` | No schema evolution; run won't fail, it just ignores schema changes. New columns are rescued only if `cloudFiles.rescuedDataColumn` is set. **Default when a schema is provided.** |
# MAGIC
# MAGIC > **Interview takeaway:** schema goes with `schemaLocation` (read), reliability goes with `checkpointLocation` (write); the evolution mode only decides what to do **when the file schema changes** – evolve, widen, rescue, fail, or ignore.

# COMMAND ----------

# DBTITLE 1,Enable type widening on the target table
# MAGIC %sql
# MAGIC ALTER TABLE learning.dlt_schema.autoloadorder
# MAGIC SET TBLPROPERTIES ('delta.enableTypeWidening' = true)

# COMMAND ----------

# DBTITLE 1,Enabling the schemaEvolution on target table
# MAGIC %sql
# MAGIC ALTER TABLE learning.dlt_schema.autoloadorder
# MAGIC SET TBLPROPERTIES (
# MAGIC     'spark.databricks.delta.schema.autoMerge.enabled' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,reading the csv with schema changes
# Auto Loader -> Delta table, with automatic schema evolution enabled
(
    spark.readStream
    .format("cloudFiles")

    # --- source / file options (readStream side) ---
    .option("cloudFiles.format", "csv")                     # how to parse each file
    .option("cloudFiles.inferColumnTypes", True)             # infer real types (int/timestamp), not all strings
    .option("header", True)                                  # first row is the header
    .option("cloudFiles.schemaLocation",
            "/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/orderSchema/")  # stores the inferred schema (_schemas/0)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # (default) evolve schema when new columns arrive
    .load("/Volumes/learning/dlt_schema/rawfilesystem/Landing/order_sample/")

    .writeStream

    # --- sink / reliability options (writeStream side) ---
    .option("checkpointLocation",
            "/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/orderCheckpoint/")  # offsets + commits + file-listing state
    .trigger(availableNow=True)
    .option('mergeSchema',True)                             # process everything available once, then stop
    .toTable("learning.dlt_schema.autoloadorder")           # target Delta table (created if missing)
)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from learning.dlt_schema.autoloadorder where _rescued_data is not Null

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS learning.dlt_schema.autoloadorder;

# COMMAND ----------

dbutils.fs.ls('/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/orderSchema//_schemas/')
dbutils.fs.head('/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/orderSchema//_schemas/0')
dbutils.fs.head('/Volumes/learning/dlt_schema/rawfilesystem/Landing/autoLoader/orderSchema//_schemas/1')