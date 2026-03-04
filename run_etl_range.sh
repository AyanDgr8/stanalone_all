#!/bin/bash

# Usage: ./run_etl_range.sh 2025-08-01 2025-08-05
# Both start and end dates are inclusive.

START_DATE=$1
END_DATE=$2

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
    echo "Usage: ./run_etl_range.sh <start_date> <end_date>"
    echo "Example: ./run_etl_range.sh 2025-08-01 2025-08-05"
    exit 1
fi

current_date="$START_DATE"
start_seconds=$(date -d "$START_DATE" +%s)
end_seconds=$(date -d "$END_DATE" +%s)

if [ $? -ne 0 ]; then
    echo "Error: Invalid date format."
    exit 1
fi

if [ $start_seconds -gt $end_seconds ]; then
    echo "Error: Start date ($START_DATE) is after End date ($END_DATE)."
    exit 1
fi

echo "Starting ETL batch for range: $START_DATE to $END_DATE"

while [ $(date -d "$current_date" +%s) -le $end_seconds ]; do
    echo "--------------------------------------------------"
    echo "Processing Date: $current_date"
    echo "--------------------------------------------------"
    
    # Run the ETL for the single day (Start = End = current_date)
    docker compose exec etl_service python etl_service.py --start-date "$current_date" --end-date "$current_date"
    
    if [ $? -ne 0 ]; then
        echo "ERROR: ETL failed for $current_date. Stopping."
        exit 1
    fi
    
    # Increment date by 1 day
    current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
    
    # Optional: slight pause to let system settle
    sleep 2
done

echo "--------------------------------------------------"
echo "Batch processing complete."
