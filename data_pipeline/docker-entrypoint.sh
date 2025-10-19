#!/bin/bash
set -e

echo "Starting cron service..."

# Load crontab
crontab /app/data_pipeline/crontab

# Print loaded crontab for verification
echo "Loaded crontab:"
crontab -l

# Create log file if it doesn't exist
touch /app/logs/cron.log

# Start cron in foreground mode and tail the log
echo "Cron service started. Tailing logs..."
cron && tail -f /app/logs/cron.log
