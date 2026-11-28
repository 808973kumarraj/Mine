# Brief: how Databricks works

Claims shown or said in the video. Sources are public Databricks docs pages, written from prior knowledge; not re-fetched in this session, so check them before publishing.

| Claim | Source |
|---|---|
| Lakehouse combines data lake storage with warehouse features | https://docs.databricks.com/aws/en/lakehouse/ |
| Data sits as Parquet files in cloud object storage (S3, ADLS, GCS) | https://docs.databricks.com/aws/en/delta/ |
| Delta Lake adds a transaction log, ACID transactions and time travel | https://docs.databricks.com/aws/en/delta/ |
| Control plane (UI, notebooks, jobs, cluster manager) vs compute plane in your cloud account | https://docs.databricks.com/aws/en/getting-started/overview |
| Cluster = driver plus workers; driver plans, workers run tasks | https://docs.databricks.com/aws/en/compute/ |
| Unity Catalog: catalog.schema.table names and permission checks | https://docs.databricks.com/aws/en/data-governance/unity-catalog/ |
| Bronze, silver, gold (medallion) stages | https://docs.databricks.com/aws/en/lakehouse/medallion |

## Illustrative (made up for the picture)
- "2 copies" in the typical setup, scene 1
- Log entry names and contents in scene 3 (labelled ILLUSTRATIVE)
- 4 workers, 12 tasks in scene 5; analyst and guest users in scene 6
- Serverless compute runs in Databricks' account, so "compute in your cloud" describes classic compute.
