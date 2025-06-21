# Databricks notebook source
# MAGIC %md
# MAGIC # Automating Workflow Jobs with Schedules and Triggers
# MAGIC
# MAGIC In **Lakeflow Jobs**, it is possible to configure jobs to automatically trigger in any of the following situations:
# MAGIC
# MAGIC - **On a time-based schedule**
# MAGIC - **On the arrival of files** to a Unity Catalog storage location
# MAGIC - **Continuously**
# MAGIC
# MAGIC You can also trigger job runs **manually** or through **external orchestration tools**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Job Schedules and Triggers
# MAGIC
# MAGIC | **Trigger Type** | **Behavior** |
# MAGIC |------------------|--------------|
# MAGIC | **Scheduled** | Triggers a job run based on a time-based schedule. |
# MAGIC | **File arrival** | Triggers a job run when new files arrive in a monitored Unity Catalog storage location. |
# MAGIC | **Continuous** | To keep the job always running, trigger another job run whenever a job run completes or fails.|
# MAGIC | **None (manual)** | Runs are triggered manually with the **Run now** button or programmatically using other orchestration tools. |
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔄 What is Auto Loader in Databricks?
# MAGIC
# MAGIC **Auto Loader** is a feature in **Databricks** used for **incrementally and efficiently ingesting new data files** as they arrive in cloud storage (like AWS S3, Azure Data Lake, or GCS) into Delta Lake tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Key Highlights
# MAGIC
# MAGIC | Feature              | Description                                                                 |
# MAGIC |----------------------|-----------------------------------------------------------------------------|
# MAGIC | **Incremental**       | Automatically detects and loads only **new files** added to a directory.    |
# MAGIC | **Scalable**          | Designed to **scale to millions of files**, better than using `read`.       |
# MAGIC | **Schema Evolution**  | Can **automatically detect new columns** and update the schema if enabled.  |
# MAGIC | **State Management**  | Tracks file ingestion state with **checkpoints** to avoid duplicates.       |
# MAGIC | **Optimized**         | Uses **cloud-specific APIs** for faster listing (e.g., Azure Event Grid).   |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📦 Supported File Formats
# MAGIC * CSV
# MAGIC * JSON
# MAGIC * Parquet
# MAGIC * Avro
# MAGIC * ORC
# MAGIC * Binary
# MAGIC
# MAGIC ---
# MAGIC ## 📥 Auto Loader `readStream` Options
# MAGIC
# MAGIC | **Option**                        | **Description**                                         | **Example**            |
# MAGIC | --------------------------------- | ------------------------------------------------------- | ---------------------- |
# MAGIC | `cloudFiles.format`               | File format (`csv`, `json`, `parquet`, etc.)            | `"csv"`                |
# MAGIC | `cloudFiles.schemaLocation`       | Required location to store schema metadata              | `"/mnt/schema/bronze"` |
# MAGIC | `cloudFiles.inferColumnTypes`     | Auto infer column types from data (CSV/JSON only)       | `"true"`               |
# MAGIC | `cloudFiles.includeExistingFiles` | Process existing files on first run                     | `"true"`               |
# MAGIC | `cloudFiles.schemaEvolutionMode`  | Auto schema evolution mode (`addNewColumns`)            | `"addNewColumns"`      |
# MAGIC | `cloudFiles.allowOverwrites`      | Allow file overwrites (if applicable)                   | `"true"`               |
# MAGIC | `cloudFiles.useNotifications`     | Use notification-based file discovery (Event Grid / S3) | `"true"`               |
# MAGIC | `cloudFiles.connectionString`     | For Azure Data Lake Gen2: SAS or credentials            | `"...?sig=..."`        |
# MAGIC | `cloudFiles.partitionColumns`     | Partition columns for file layout                       | `"year,month"`         |
# MAGIC | `cloudFiles.maxBytesPerTrigger`   | Max data size read per batch                            | `"104857600"` (100MB)  |
# MAGIC | `cloudFiles.maxFilesPerTrigger`   | Max number of files read per batch                      | `"100"`                |
# MAGIC | `cloudFiles.enforceSchema`        | If false, allows missing columns instead of failing     | `"false"`              |
# MAGIC | `cloudFiles.namingHint`           | Helps improve performance in file discovery             | `"bronze-data"`        |
# MAGIC | `cloudFiles.validateOptions`      | Enables validation of format-specific options           | `"true"`               |
# MAGIC
# MAGIC ---
# MAGIC ## 📤 writeStream Options (Delta Sink)
# MAGIC | **Option**           | **Description**                                          | **Example**                           |
# MAGIC | -------------------- | -------------------------------------------------------- | ------------------------------------- |
# MAGIC | `checkpointLocation` | Required to track stream progress                        | `"/mnt/checkpoints/myjob/"`           |
# MAGIC | `path`               | Output path if not passed to `.start()`                  | `"/mnt/delta/bronze/"`                |
# MAGIC | `mergeSchema`        | Merge new schema with existing Delta schema              | `"true"`                              |
# MAGIC | `outputMode`         | Output mode: `append`, `complete`, `update`              | `"append"`                            |
# MAGIC | `trigger`            | Controls how frequently batches are triggered            | `Trigger.ProcessingTime("5 minutes")` |
# MAGIC | `maxFilesPerTrigger` | Throttles file processing rate                           | `"100"`                               |
# MAGIC | `maxBytesPerTrigger` | Limits total bytes per micro-batch                       | `"104857600"`                         |
# MAGIC | `ignoreChanges`      | Ignore updates for existing rows (for idempotent writes) | `"true"`                              |
# MAGIC | `ignoreDeletes`      | Ignore delete operations if using upserts                | `"true"`                              |
# MAGIC | `replaceWhere`       | Overwrite subset of data (if `overwrite` mode used)      | `"year=2024"`                         |
# MAGIC | `partitionBy`        | Partition columns for Delta sink                         | `["year", "month"]`                   |
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Types of Auto Loader Triggers in Databricks
# MAGIC | Trigger Type     | Description                                   | Code Example                             | Use case |
# MAGIC | ---------------- | --------------------------------------------- | ---------------------------------------- |------------------
# MAGIC | `once`           |**one time micro batch.** Runs once, processes all available data       | `.trigger(once=True)`                    | Nightly or ad-hoc runs when files arrive at a known time. |
# MAGIC | `processingTime` | **fixed interval micro batch.** Runs every interval (e.g., 5 min, 1 hr)       | `.trigger(Trigger.ProcessingTime("5m"))` |Near real-time ingestion with controlled resource usage. |
# MAGIC | `availableNow`     | **Default.** Runs as many batches as needed to process all currently available data, then stops. | `.trigger(availableNow=True)`     | 
# MAGIC
# MAGIC ---
# MAGIC ## 🔧 How It Works
# MAGIC
# MAGIC ```python
# MAGIC
# MAGIC from pyspark.sql.functions import input_file_name, current_timestamp
# MAGIC
# MAGIC # Step 1: Read from cloud storage using Auto Loader
# MAGIC df = (
# MAGIC   spark.readStream
# MAGIC   .format("cloudFiles")
# MAGIC   .option("cloudFiles.format", "csv")  # Change to "json", "parquet", etc. if needed
# MAGIC   .option("cloudFiles.schemaLocation", "/mnt/schema/sales/")  # Tracks schema changes
# MAGIC   .option("header", "true")  # Assuming CSV has headers
# MAGIC   .load("/mnt/raw/mydata/")  # Folder with 100+ files
# MAGIC )
# MAGIC
# MAGIC # Step 2: Add metadata columns (file name and load timestamp)
# MAGIC df_with_metadata = df.withColumn("Metadata_source_file_name", input_file_name()) \
# MAGIC                      .withColumn("Metadata_load_timestamp", current_timestamp())
# MAGIC
# MAGIC # Step 3: Write to Delta table
# MAGIC df_with_metadata.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/mnt/checkpoints/sales_bronze/") \
# MAGIC     .start("/mnt/delta/sales_bronze/")  # You can also use a managed table path
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ## 🔄 Auto Loader vs COPY INTO 
# MAGIC | Feature / Aspect        | 🔄 **Auto Loader**                                          | 📥 **COPY INTO**                                     |
# MAGIC | ----------------------- | ----------------------------------------------------------- | ---------------------------------------------------- |
# MAGIC | **Ingestion Mode**      | **Streaming** (incremental, continuous or trigger-based)    | **Batch** (manual or scheduled execution)            |
# MAGIC | **Source Monitoring**   | Watches a directory for **new files continuously**          | Does **not track** files; must re-check manually     |
# MAGIC | **State Management**    | Uses **checkpointing** to track ingested files              | Tracks files using **audit log table metadata**      |
# MAGIC | **Latency**             | Near real-time (trigger every few seconds/minutes)          | Manual or scheduled; **higher latency**              |
# MAGIC | **Schema Evolution**    | Supported (with `addNewColumns` mode)                       | ❌ Not natively supported                             |
# MAGIC | **File Deduplication**  | Built-in using file IDs and checksums                       | Avoids reloading by checking file metadata hash      |
# MAGIC | **Trigger Options**     | `once`, `processingTime`, `availableNow`, `continuous`      | No streaming — must be triggered via SQL or jobs     |
# MAGIC | **Format Support**      | CSV, JSON, Parquet, Avro, ORC, Binary                       | Same (via `FILEFORMAT`)                              |
# MAGIC | **Ease of Use**         | Ideal for large-scale pipelines with many incoming files    | Simpler for small, ad-hoc, or periodic loads         |
# MAGIC | **Cost Efficiency**     | Efficient for **frequent ingestion** of large directories   | Better for **one-time** or low-frequency batch loads |
# MAGIC | **Catalog Integration** | Fully compatible with Unity Catalog + file arrival triggers | Compatible with Unity Catalog (manual registration)  |
# MAGIC ---
# MAGIC
# MAGIC ``` sql
# MAGIC
# MAGIC COPY INTO my_table
# MAGIC FROM '/mnt/raw/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
