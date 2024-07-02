# System Architecture
![SCR-20240630-a51](https://github.com/quang08/Smart-City/assets/84165564/38120d7c-a561-4303-a787-08367629516b)

- **Data Ingestion**: Data from various sources are streamed into Kafka.
- **Real-time Processing**: Spark listens and consumes from Kafka then process data.
- **Storage**: Processed data is streamed and stored to AWS S3 buckets.
- **ETL and Metada Management**: AWS Glue extracts data from S3 and maintains the data in Data Catalog.
- **Querying and Analysis**: AWS Athena can query Data Catalog tables, while AWS Redshift as the Data Warehouse that loads data from Data Catalog for further analytics.