# Docker Setup for Stock Data Ingestion

This Docker setup provides automated daily stock data ingestion using a cron job container.

## Services

### 1. `stock-data-cron` (Main Service)
- Runs a cron job that executes daily at 6:00 PM EST (after market close)
- Automatically ingests stock data for NYSE and NASDAQ exchanges
- Persists data, logs, and CSV exports through Docker volumes

### 2. `stock-data-runner` (Manual Execution)
- Optional service for manual/one-time execution
- Runs with the `manual` profile

## Configuration

### Environment Variables
Create a `.env` file in the project root with the following variables:

```env
DATA_PROVIDER=yfinance
DB_TYPE=sqlite
MAX_WORKERS=15
```

### Cron Schedule
The default schedule is set to run daily at 6:00 PM EST. To modify:
1. Edit the `crontab` file
2. Rebuild the Docker image

Current schedule format:
```
# minute hour day month day_of_week command
0 18 * * * cd /app && python cron_job.py >> /app/logs/cron.log 2>&1
```

## Usage

### Start the Cron Service
```bash
docker-compose up -d
```

### View Logs
```bash
# View cron service logs
docker-compose logs -f stock-data-cron

# View application logs inside container
docker exec stock-data-cron tail -f /app/logs/cron.log
```

### Run Manual Execution
```bash
docker-compose --profile manual run --rm stock-data-runner
```

### Stop Services
```bash
docker-compose down
```

### Rebuild After Changes
```bash
docker-compose build
docker-compose up -d
```

## Data Persistence

The following directories are mounted as volumes:
- `./data` - SQLite database files
- `./logs` - Application and cron logs
- `./csv_exports` - Exported CSV files

## Monitoring

Check if the cron job is running:
```bash
docker exec stock-data-cron crontab -l
```

View recent cron executions:
```bash
docker exec stock-data-cron tail -100 /app/logs/cron.log
```

## Troubleshooting

1. **Container not starting**: Check logs with `docker-compose logs stock-data-cron`
2. **Cron not executing**: Verify crontab is loaded with `docker exec stock-data-cron crontab -l`
3. **Database issues**: Ensure the `./data` directory has proper permissions
4. **Time zone issues**: The container uses `America/New_York` timezone by default

## Production Considerations

1. **Backup Strategy**: Regularly backup the `./data` directory
2. **Log Rotation**: Implement log rotation for `/app/logs/cron.log`
3. **Monitoring**: Set up alerts for failed cron executions
4. **Resource Limits**: Add memory and CPU limits in docker-compose.yaml if needed

