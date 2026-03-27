# Crypto ETL Framework (Bitstamp + Live API)
This project is a functional Data Engineering pipeline designed to bridge the gap between historical crypto archives and live market data. It uses Apache Airflow to orchestrate a Medallion Architecture, moving data from raw CSVs and REST APIs into a structured PostgreSQL warehouse.

## 📂 Project Structure
```text
.
├── dags/
│   └── bitcoin_etl_dag.py       # Airflow DAG defining the pipeline logic
├── include/
│   ├── data/
│   │   ├── .gitkeep             # Keeps folder alive without the 373MB CSV
│   │   └── bitstamp.csv         # (Local only) Raw historical data
│   ├── screenshots/             # Documentation for UI and Pipeline
│   └── scripts/
│       ├── transformer.py       # Python logic for cleaning & idempotency
│       ├── live_api.py          # Script for fetching CoinGecko data
│       └── schema_setup.sql     # SQL to initialize Postgres tables
├── dashboard.py                 # Streamlit application code
├── error_alerts.txt             # Local file-based error logging
├── airflow_settings.yaml        # Astro CLI Docker configuration
├── requirements.txt             # Python libraries (Pandas, SQLAlchemy etc.)
└── README.md                    # Project documentation
```

## 🚀 Engineering Features
### 1. Hybrid Ingestion (Kaggle + Live)
The pipeline merges two massive data gaps:

Historical (Bronze): Processes 10 years of OHLCV data from the Bitstamp Kaggle Dataset (2012–2021).

Live (Bronze): Minute-by-minute price updates fetched via CoinGecko API for current 2026 market cycles.

### 2. Idempotent Data Loading
To ensure the pipeline is "re-runnable" without corrupting data, the transformer.py uses a Delete-before-Append strategy. It identifies the timestamp range of the incoming batch and purges existing records in that window before inserting new ones.

### 3. Dual-Layer Alerting System
I implemented a redundant monitoring system to ensure 99.9% uptime:

Slack Alerts: Sends real-time JSON payloads to a dedicated workspace on task failure.

Local Fallback: If Slack is unreachable, errors are appended to include/logs/error_alerts.txt for local debugging.

### 4. Medallion Architecture Logic
Bronze Cleanup: Raw Unix timestamps are converted to readable dates and null prices are handled via ffill() (forward fill) to account for low-volume trading gaps.

Silver (Daily Summary): Uses SQL UPSERT (ON CONFLICT) to aggregate raw ticks into daily High/Low/Avg prices.

Gold (Analytics): Dynamic views calculating 7-day rolling averages and volatility indices.

### 5. Scheduling
The DAG is pinned to a Midnight (00:00) schedule using Crontab expressions to ensure daily summaries are calculated precisely when the market "closes" for the day.

## 📊 Results & Visualization
### Airflow DAG Orchestration
The pipeline is scheduled at Midnight (00:00) to process the previous day's data, ensuring clean daily partitions.

### Redundant Monitoring (Slack)
Detailed error reporting caught during the development of the create_daily_summary task, showing failed column mappings.

### Analytics Dashboard
The final Streamlit UI displaying the current market state and historical convergence.

## Setup
**Prerequisites**: Install Astro CLI.

**Data**: Place bitstamp.csv in include/data/.

**Secrets**: Rename airflow_settings.yaml.example to airflow_settings.yaml and add Slack/Postgres credentials.

**Launch**: astro dev start -> streamlit run include/dashboard.py.