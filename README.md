# LSEG ESG Scores Scraper

This project implements a threaded pipeline to collect and transform **Environmental, Social, and Governance (ESG)** scores for public companies from the **London Stock Exchange Group (LSEG)** ESG portal. It extracts ESG scores for listed companies based on their RIC (Reuters Instrument Code), processes the results into a normalized tabular format, and loads the data into a Microsoft SQL Server table.

## Overview

### Objective

This scraper is intended for organizations or analysts who require daily or periodic access to LSEG's proprietary ESG scoring data. The pipeline is optimized for speed via multithreading and supports SQL Server-based storage for enterprise workflows.

## Data Source

- **Base Endpoint**: `https://www.lseg.com/bin/esg/`
- **Discovery Endpoint**: `/esgsearchsuggestions/` – fetches a list of instrument RICs
- **Score Endpoint**: `/esgsearchresult/` – retrieves ESG data for each RIC

The scraper fetches ESG metrics including:
- Name, Ticker, Exchange
- Environmental, Social, Governance pillar scores and weights
- Peer industry comparisons

## Output Fields

Transformed records include the following key columns:

- `name`: Full name of the entity
- `ext8_ticker`: Raw RIC format (e.g., "XYZ.O")
- `ticker`: Ticker stripped of exchange (e.g., "XYZ")
- `exchange`: Exchange portion of the RIC (e.g., "O")
- `esg_environmental`, `esg_social`, `esg_governance`: ESG sub-score values
- `esg_environmental_weight`, `esg_social_weight`, etc.: Score weights
- `timestamp_created_utc`: Time of ingestion
- Additional fields from `industryComparison` (e.g., `esg_score`, `percentile`, `sector_average`)

## Pipeline Architecture

1. **Initialization**  
   Launches the scraping engine with multithreaded support (default: 10 threads).

2. **RIC Fetching**  
   Discovers a list of instrument tickers from the `/esgsearchsuggestions/` endpoint.

3. **ESG Score Retrieval**  
   Each thread fetches and parses ESG scores from the `/esgsearchresult/` endpoint.

4. **Data Transformation**  
   Converts nested JSON responses into a flattened tabular format.

5. **Database Insertion**  
   Inserts the resulting DataFrame into a Microsoft SQL Server table.

## Project Structure

```
lseg-client-main/
├── main.py                   # Pipeline entrypoint
├── core/                     # Data fetching and request logic
│   ├── lseg.py
│   └── request.py
├── transformer/              # Data flattener and cleaner
│   └── agent.py
├── database/                 # MSSQL writer
│   └── mssql.py
├── config/                   # Logging and settings
│   ├── logger.py
│   └── settings.py
├── .env.sample               # Sample environment variables
├── Dockerfile                # Container setup
├── requirements.txt          # Dependencies
```

## Configuration

Create a `.env` file using `.env.sample` as a reference. Key variables include:

| Variable | Description |
|----------|-------------|
| `THREAD_COUNT` | Number of concurrent threads for scraping |
| `LOG_LEVEL` | Logging verbosity (`INFO`, `DEBUG`, etc.) |
| `OUTPUT_TABLE` | SQL Server table where data will be inserted |
| `MSSQL_SERVER`, `MSSQL_DATABASE`, `MSSQL_USERNAME`, `MSSQL_PASSWORD` | MSSQL connection |
| `INSERTER_MAX_RETRIES` | Retry attempts for database failures |
| `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Scraper backoff config |

Optional proxy support is available via BrightData environment variables.

## Docker Support

Build and run using Docker:

```bash
docker build -t lseg-client .
docker run --env-file .env lseg-client
```

## Local Installation

Install dependencies with:

```bash
pip install -r requirements.txt
```

Run the application:

```bash
python main.py
```

## Logging & Output

Logs include:
- Number of RICs discovered
- Threaded progress and completion
- Transformation diagnostics
- Insertions and database connection status
