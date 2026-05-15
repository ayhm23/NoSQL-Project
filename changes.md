# Hadoop & Pig Setup Changes Log

This document summarizes the exact code and configuration changes made to the project to successfully run the Apache Pig MapReduce pipeline on the local Mac M2 environment.

## 1. Source Code Changes

### `pipelines/pig_pipeline.py`
- **Fixed `AttributeError` (Tuple unpacking)**: The `log_parser.parse_line()` function returns a tuple `(LogRecord, malformed_flag)`, but the pipeline originally treated it as a single dictionary.
  - *Fix:* Added `record_obj, malformed = parse_line(raw_line)` and passed `record_obj.to_dict()` into `_record_to_row()`.
- **Fixed `FileNotFoundError` (Binary Paths)**: `subprocess.run()` couldn't find `pig` and `hdfs` binaries because it didn't inherit the `.zshrc` `PATH`.
  - *Fix:* Replaced `"hdfs"` with `f"{config.HADOOP_HOME}/bin/hdfs"` and `"pig"` with `f"{config.PIG_HOME}/bin/pig"`.
- **Fixed Pig Local FS Defaulting (HDFS connection)**: Pig was defaulting to the Mac's local filesystem (`file:///`) because it couldn't locate `core-site.xml`.
  - *Fix:* Injected `HADOOP_CONF_DIR=f"{config.HADOOP_HOME}/etc/hadoop"` directly into the `pig_env` dictionary passed to `subprocess.run()`.

### `main.py`
- **Registered the Pig Pipeline**: 
  - *Fix:* Added `pipelines.pig_pipeline.PigPipeline` into the lazy-loading `_get_pipeline_class` factory method so that the `--pipeline pig` CLI flag could successfully trigger the pipeline instead of throwing a `NotImplementedError`.

### `config.py`
- **Added Hadoop/Pig Global Constants**: 
  - *Fix:* Appended `HADOOP_HOME`, `PIG_HOME`, `PIG_SCRIPTS_DIR`, and `PIG_HDFS_BASE` to centralize the environment paths dynamically.

---

## 2. Hadoop Configuration Changes

All changes were made in `/opt/homebrew/opt/hadoop/libexec/etc/hadoop/`.

### `yarn-site.xml`
The most critical modifications were made here to bypass aggressive Mac local resource checks that caused MapReduce jobs to freeze forever.
- Disabled disk health checker: `<name>yarn.nodemanager.disk-health-checker.enable</name>` set to `false`.
- Disabled virtual/physical memory limits: `<name>yarn.nodemanager.vmem-check-enabled</name>` and `pmem-check-enabled` set to `false`.
- **The Golden Fix**: Set `<name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>` to `99.9`. This prevented `LocalDirAllocator` from refusing to launch map-reduce containers when the Mac's SSD utilization exceeded 90%.

### `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`
- Configured pseudo-distributed execution pointing to `hdfs://localhost:9000` and `yarn` framework rather than standalone local execution.

---

## 3. Environment & System Changes

### `~/.zshrc`
- Set `JAVA_HOME=/opt/homebrew/opt/openjdk@17` to ensure Hadoop 3.4.x was running against the correct JVM class version (fixing `UnsupportedClassVersionError: 61.0`).
- Exported `HADOOP_HOME` and `PIG_HOME` to the system `PATH`.
- Exported a massive `PIG_CLASSPATH` pointing to all `/opt/homebrew/opt/hadoop/libexec/share/hadoop/` jar directories, preventing Pig 0.17 from throwing `ClassNotFoundException` against Hadoop 3.

### Virtual Environment
- Established a local Python `venv`.
- Fixed `ModuleNotFoundError` for `pymongo` and other database drivers by running `pip install -r requirements.txt`.
- Deleted interrupted, corrupted `NASA_access_log` `.gz` files that bypassed the download check but crashed the parser.
