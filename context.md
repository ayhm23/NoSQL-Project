# NoSQL-Project — Repository Context

## 1. Project Overview & Objective

This project is a big-data ETL (Extract, Transform, Load) pipeline for analyzing the **NASA HTTP Web Server Logs** from July and August 1995. The primary objective is to build and compare multiple distributed data processing frameworks—specifically **MongoDB**, **Hadoop MapReduce**, **Apache Pig** (planned), and **Apache Hive** (planned)—against the same set of queries. 

The pipeline parses the massive raw server logs, runs complex queries (like finding the most frequent visitors or generating TF-IDF-like co-occurrence indexes depending on the specific query tasks), and stores the aggregated results and execution times in a relational database (PostgreSQL/MySQL) for comparative reporting.

---

## 2. Architecture & Data Flow

The project acts as an automated harness that feeds data through one of the selected pipelines.

```mermaid
graph TD
    A[NASA Log Files (gz)] -->|parser/| B[Log Batcher]
    B --> C{Pipeline Engine}
    
    C -->|pipelines/mongo_pipeline| D[MongoDB]
    C -->|pipelines/mapreduce_pipeline| E[Hadoop MapReduce]
    C -->|Stub| F[Pig / Hive]
    
    D -->|Aggregations| G[Result Sets & Metrics]
    E -->|Map/Reduce Output| G
    
    G -->|db/loader.py| H[(Relational DB: PostgreSQL/MySQL)]
    H -->|reporting/| I[Comparative Report Generation]
```

### 2.1 The Pipeline Lifecycle
1. **Extraction**: Raw HTTP logs are extracted and decompressed from `data/NASA_access_log_*.gz`.
2. **Transformation**: The `parser/` module cleans and extracts fields (IP, timestamp, HTTP method, endpoint, status code, bytes).
3. **Load / Processing**: 
   - *MongoDB Pipeline*: Batches records and uses MongoDB Aggregation Pipelines to process queries.
   - *MapReduce Pipeline*: Uses Hadoop Streaming with Python scripts (`mapreduce/`) to map and reduce the data.
4. **Benchmarking**: The `db/loader.py` script saves the final answers and the exact execution times to a central SQL database.
5. **Reporting**: `reporting/report.py` queries the SQL database to output performance comparisons across the engines.

---

## 3. Directory Structure

```
NoSQL-Project/
├── main.py                  # CLI entry point for running pipelines and reports
├── config.py                # Centralized configuration (paths, DB URIs, batch sizes)
├── download_data.sh         # Helper script to pull NASA logs from the ITA archive
├── requirements.txt         # Python dependencies
├── run_pipeline.txt         # Execution instructions/reference
│
├── data/                    # (Not in VC) Raw .gz logs downloaded from the web
│
├── parser/
│   ├── log_parser.py        # Regex logic for parsing the Apache Common Log Format
│   └── batcher.py           # Yields chunks of parsed logs to manage memory
│
├── pipelines/
│   ├── base_pipeline.py     # Abstract base class defining the required interface
│   ├── mongo_pipeline.py    # MongoDB aggregation pipeline implementation
│   └── mapreduce_pipeline.py# Hadoop MapReduce orchestrator
│
├── mapreduce/               # Python-based Hadoop Streaming scripts
│   ├── q1_mapper.py / q1_reducer.py
│   ├── q2_mapper.py / q2_reducer.py
│   └── q3_mapper.py / q3_reducer.py
│
├── db/                      # Relational DB integration for benchmarking
│   ├── init_db.py           # Initializes Postgres/MySQL tables
│   ├── schema.sql           # SQL table definitions
│   └── loader.py            # Inserts pipeline results and timings
│
├── reporting/
│   └── report.py            # Queries the SQL DB to generate comparison tables
│
└── tests/                   # Unit/integration tests
```

---

## 4. Key Components Explained

### 4.1 Configuration (`config.py`)
Provides a single source of truth for all tunable parameters, utilizing environment variables with sensible defaults. Covers batch sizes (`50,000`), MongoDB URIs, and RDBMS connection details.

### 4.2 The CLI (`main.py`)
Orchestrates the entire system.
- `--pipeline [mongodb|mapreduce|pig|hive]`: Selects the engine.
- `--batch-size`: Overrides default batching.
- `--report` / `--report-only`: Controls the generation of the final comparison report.
- Example: `python main.py --pipeline mapreduce --report`

### 4.3 Log Parsing (`parser/`)
NASA logs are messy and sometimes malformed. `log_parser.py` safely extracts:
- `ip`: Client identifier
- `timestamp`: Datetime of request
- `method`: GET, POST, etc.
- `url`: The requested resource
- `status`: HTTP status code
- `size`: Bytes transferred

### 4.4 The MongoDB Pipeline (`pipelines/mongo_pipeline.py`)
Uses `pymongo` to insert batches of logs into a MongoDB collection (`raw_logs`). It then leverages MongoDB's powerful `aggregate()` framework (using `$match`, `$group`, `$sort`, etc.) to answer the queries entirely within the database engine.

### 4.5 The MapReduce Pipeline (`pipelines/mapreduce_pipeline.py` & `mapreduce/`)
Executes distributed queries using Hadoop Streaming. 
- Mappers (e.g., `q1_mapper.py`) read raw lines from `stdin` and emit key-value pairs.
- Reducers (e.g., `q1_reducer.py`) read sorted key-value pairs from `stdin`, aggregate them, and emit the final results.
- `mapreduce_pipeline.py` orchestrates the local or distributed execution of these scripts.

### 4.6 Reporting and DB (`db/` & `reporting/`)
To properly compare the execution time of MongoDB vs. MapReduce, results are unified. `loader.py` commits the answers and time taken to an RDBMS. `report.py` then generates a clean console or CSV report showing which engine performed better for each query type.

---

## 5. Setup & Execution

### Initial Setup
1. **Database Requirements**: Ensure MongoDB is running locally, and either PostgreSQL or MySQL is available for benchmarking results.
2. **Environment**: Install dependencies `pip install -r requirements.txt`.
3. **Data**: The data will automatically download via `main.py` if missing, or you can run `./download_data.sh` manually.

### Running a Pipeline
To run the MongoDB pipeline and view the report:
```bash
python main.py --pipeline mongodb --report
```

To run the MapReduce pipeline:
```bash
python main.py --pipeline mapreduce --report
```

To view the benchmark comparison without running a new ETL job:
```bash
python main.py --pipeline mongodb --report-only
```
