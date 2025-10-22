## Future Scope:

### Functional:
- support multiple custom indices (Equal Weight 100, Equal Weight Tech 50, etc)
- support different strategies with Other Weighting Methods. (Market Cap Weighted, Equal Share Weighted, etc)
- support multiple data providers

- Improve change tracking. instead of { date: portfolio }. takes space.

### Techincal:
- Use Data Warehouse, instead of DB to Scale. 

- Rate Limit web service.

- Health check endpoint.
- Add more workers for api.
- Async requests (Done)

- Use multi-stage Docker builds for smaller images. (explore)

- Avoid storing large files in DB; use GCS buckets (Data lakes . hudi iceberg etc. ) for reports if needed.

- Prometheus + Grafana (if on GKE) or Cloud Monitoring
- Monitoring alerts for failures

- Make Connection Objects singleton OR added context manager support to avoid multiple connections being opened.




