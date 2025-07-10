# telegram-data-pipeline
End-to-end data pipeline for analyzing Ethiopian medical businesses using Telegram data.

# Telegram Data Pipeline

## Project Overview
This project builds a data pipeline to scrape Telegram channel messages, store them in PostgreSQL, and transform them into a star schema using dbt for analysis. It uses Docker Compose for environment setup and includes image processing with YOLOv8 (Task 3). The pipeline processes 476 messages from channels like `tenamereja`, creating staging and mart layers for querying.

## Prerequisites
- Docker and Docker Compose
- Python 3.10)
- PostgreSQL 14
- dbt 1.9.0-b4 with dbt-postgres
- Git

## Prerequisites
- Docker and Docker Compose
- Python 3.10)
- PostgreSQL 14
- dbt 1.9.0-b4 with dbt-postgres
- Git

## Project Structure
```
telegram_data_pipeline/
├── data/
│   ├── raw/
│   │   └── telegram_messages/  # JSON files and images from Telegram
├── telegram_data_pipeline/  # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_telegram_messages.sql  # Staging model
│   │   ├── marts/
│   │       ├── dim_channels.sql  # Dimension: channels
│   │       ├── dim_dates.sql    # Dimension: dates (2020–2025)
│   │       ├── fct_messages.sql # Fact: messages
│   │       └── schema.yml       # Tests for models
│   ├── tests/
│   │   └── non_negative_message_length.sql  # Custom test
│   └── dbt_project.yml  # dbt config
├── scrape_telegram.py  # Scrapes Telegram messages
├── load_json_to_postgres.py  # Loads JSON to PostgreSQL
├── Dockerfile  # Python environment
├── docker-compose.yml  # Docker services
├── requirements.txt  # Python dependencies
└── .gitignore  # Excludes sensitive files

```

## Setup Instructions
1. **Clone the Repository**:
   ```bash
   git clone <https://github.com/moheranus/telegram-data-pipeline>
   cd telegram-data-pipeline
   ```
2. **Set Up Environment**:
   - Create a `.env` file with:
     ```
     POSTGRES_USER=admin
     POSTGRES_PASSWORD=securepassword123
     POSTGRES_DB=telegram_data_warehouse
     ```
   - Build and start containers:
     ```powershell
     docker-compose up -d
     ```

3. **Configure dbt**:
   - Create `profiles.yml` in the project root:
     ```yaml
     telegram_data_pipeline:
       target: dev
       outputs:
         dev:
           type: postgres
           host: postgres
           user: admin
           password: securepassword123
           port: 5432
           dbname: telegram_data_warehouse
           schema: public
           threads: 4
     ```
   - Mount in `docker-compose.yml`:
     ```yaml
     volumes:
       - .:/app
       - ./profiles.yml:/root/.dbt/profiles.yml
     ```

## Task Descriptions

### Task 0: Environment Setup
- **Goal**: Create a reproducible environment.
- **Steps**:
  - Configured `docker-compose.yml` for `app` (Python 3.10) and `postgres` (PostgreSQL 14) services.
  - Added port 8080 for dbt docs.
  - Set up `Dockerfile` with dbt and Python dependencies.
- **Verification**: Run `docker ps` to confirm containers; connect to PostgreSQL with `psql -U admin -d telegram_data_warehouse`.

### Task 1: Data Ingestion
- **Goal**: Scrape and load Telegram messages.
- **Steps**:
  - Developed `scrape_telegram.py` to save messages as JSON and images to `data/raw/telegram_messages`.
  - Created `load_json_to_postgres.py` to load JSON into `raw.telegram_messages` (476 rows).
- **Usage**:
  ```bash
  python scrape_telegram.py
  python load_json_to_postgres.py
  ```
- **Verification**: Run `SELECT count(*) FROM raw.telegram_messages;` in PostgreSQL (expected: 476).

### Task 2: Data Modeling and Transformation
- **Goal**: Transform data into a star schema with dbt.
- **Steps**:
  - Created staging model (`stg_telegram_messages.sql`) to clean raw data (476 rows).
  - Built dimension tables: `dim_channels` (5 rows), `dim_dates` (2020–2025, 2192 rows).
  - Built fact table: `fct_messages` (476 rows).
  - Fixed schema issue (`public_marts` → `marts`) in `dbt_project.yml`.
  - Extended `dim_dates` to resolve test failure (76 rows).
  - Added tests in `schema.yml` (uniqueness, non-null, referential integrity) and `non_negative_message_length.sql`.
  - Generated docs with `dbt docs generate`.
- **Usage**:
  ```bash
  cd telegram_data_pipeline
  dbt run --profiles-dir ./
  dbt test --profiles-dir ./
  dbt docs generate --profiles-dir ./
  dbt docs serve --port 8080 --profiles-dir ./
  ```
- **Verification**: Check tables with `\dt staging.*; \dt marts.*;` and confirm counts (e.g., `dim_dates`: 2192 rows).

## Future Work
- **Task 3**: Implement YOLOv8 for image object detection and create `fct_image_detections`.

## Troubleshooting
- **Schema Mismatch**: Ensure `dbt_project.yml` specifies `schema: staging` and `schema: marts`.
- **dbt Docs Error**: Verify `profiles.yml` is mounted at `/root/.dbt/profiles.yml`.
- **Data Issues**: Run `SELECT count(*) FROM raw.telegram_messages;` if `0`, rerun `python load_json_to_postgres.py`.

## Contributors
- Daniel Shobe
```

</xaiArtifact>
