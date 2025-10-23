Customer Churn ETL Pipeline (Apache Airflow + AWS)

Overview

An automated ETL data pipeline built with Apache Airflow and AWS Glue to load customer churn data from Amazon S3 into Amazon Redshift, enabling analytics and visualization in Power BI.

⸻

Architecture
	•	Amazon S3: Stores raw customer churn dataset
	•	AWS Glue Crawler: Infers schema and creates Data Catalog
	•	AWS Glue Job: Loads data from S3 into Redshift
	•	Amazon Redshift: Centralized data warehouse for analytics
	•	Apache Airflow: Orchestrates and automates ETL workflow
	•	Power BI / Athena: Used for querying and visualizing insights

⸻

Workflow
	1.	Upload raw data to S3
	2.	Glue Crawler detects schema and catalogs data
	3.	Airflow DAG triggers a Glue Job that loads data into Redshift
	4.	The DAG waits until the Glue job completes using a GlueJobSensor
	5.	Connect Power BI to Redshift for churn analysis dashboards

⸻

Tools & Technologies

Python, Apache Airflow, AWS S3, AWS Glue, Amazon Redshift, Athena, Power BI


Key Highlights
	•	Fully automated weekly ETL workflow managed by Airflow
	•	Integrated monitoring via GlueJobSensor
	•	End-to-end AWS pipeline from ingestion to analytics
