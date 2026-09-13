# Taxi Data Platform

## Architecture

Source CSV files
    ↓
Bronze: taxi.bronze.yellow_tripdata
    ↓
Silver: taxi.silver.yellow_tripdata
    ↓
Gold: taxi.gold.daily_trip_metrics

## Layers

### Bronze
- Auto Loader ingestion
- Delta table
- Source-file metadata
- Append processing

### Silver
- Data-quality filtering
- Cleaned trip records
- Delta table

### Gold
- Daily trip-level business aggregates
- Daily revenue and distance metrics
- Delta table

## Validation

Bronze rows: 47,248,845
Silver rows: 47,222,631
Gold trips: 47,222,631

Silver → Gold reconciliation: PASS