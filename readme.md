# Funnel Analytics Data Pipeline

This project builds an automated funnel analytics pipeline.

## Architecture

Raw CSV → Python ETL → MySQL → Tableau Dashboard

## Features

- Automatic ingestion of multiple CSV files
- Data cleaning and transformation
- Incremental loading into MySQL
- Automatic archiving of processed files
- Tableau dashboard for visualization

## Folder Structure

data/
  raw/        # incoming data files
  processed/  # cleaned dataset
  archive/    # processed raw files

scripts/
  pipeline.py # ETL pipeline

dashboards/
  funnel_dashboard.twb

logs/
  pipeline logs

## Run Pipeline

python3 scripts/pipeline.py





--------------------------------------------

# Funnel Data Pipeline

This project builds an incremental ETL pipeline for a food ordering platform.

## Architecture

CSV → Python ETL → MySQL → Tableau Dashboard

## Features

- Incremental data loading
- Data cleaning with Pandas
- MySQL storage
- Tableau analytics dashboard

## Tech Stack

Python  
Pandas  
MySQL  
Tableau  

## Project Structure

funnel_pipeline
│
├── scripts
│   └── pipeline.py
│
├── data
│   ├── raw
│   ├── processed
│   └── archive
│
├── logs
│
├── requirements.txt
└── README.md