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