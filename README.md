Sure, here is the updated README with the new sections:

---

# Data Engineering Project - Bitcoin Analysis

This repository showcases a comprehensive data engineering project focused on Bitcoin analysis. The goal was to build a data pipeline that performs analysis on Bitcoin data by ingesting parquet files from Amazon S3 into Snowflake, transforming the data with dbt, aggregating data, and computing KPIs. The orchestration of the pipelines was managed using Apache Airflow with Cosmos.

## Overview

The primary objective of this project is to create a robust data pipeline for analyzing Bitcoin data. The pipeline involves:
- Ingesting parquet files from Amazon S3 into Snowflake.
- Running data transformations and aggregations using dbt.
- Calculating various KPIs using SQL and Python models in dbt.
- Orchestrating the entire workflow with Apache Airflow and Cosmos.

### Data Architecture

The architecture for this project was chosen to leverage the strengths of each tool:
- **Amazon S3** for scalable and cost-effective storage.
- **Snowflake** for powerful data warehousing and analytics.
- **dbt** for efficient data transformation and modeling.
- **Apache Airflow with Cosmos** for workflow orchestration and scheduling.

## Prerequisites

Before running the project, ensure you have the following prerequisites:

- Docker
- Python 3.8+
- Access to an Amazon S3 bucket containing the Bitcoin data in parquet format (AWS Public Blockchain Data available [here](https://registry.opendata.aws/aws-public-blockchain/))
- Snowflake account and necessary credentials
- Apache Airflow and Cosmos installed ([Astronomer Cosmos GitHub repository](https://github.com/astronomer/astronomer-cosmos))

## How to Run This Project

Follow these steps to set up and run the project:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/zack242/bitcoin_analysis
   cd bitcoin-analysis
   ```

2. **Start Astronomer Cosmos:**
   - Ensure Docker is running on your computer.
   - Start Airflow:
     ```bash
     astro dev start
     ```

3. **Install Python Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Snowflake and S3 Credentials:**
   - Ensure your Snowflake credentials and S3 access keys are set in the environment variables or configuration files of dbt (`~/.dbt/profiles.yml`).

5. **Run the Ingestion DAG:**
   - Trigger the ingestion DAG to load data from S3 to Snowflake.

6. **Run the Analysis DAG:**
   - Once the ingestion DAG completes, the DAG to perform data transformations and KPI calculations will start.

7. **Verify the Pipeline:**
   - Check the Airflow logs and Snowflake tables to ensure data is ingested and transformed correctly.

## Lessons Learned

Throughout the development of this project, several key lessons were learned:
- **Tool Integration:** Integrating multiple tools like Snowflake, dbt, and Airflow requires careful planning and configuration.
- **Scalability:** Using cloud services like S3 and Snowflake provides scalability and ease of management.
- **Automation:** Automating workflows with Airflow significantly improves efficiency and ensures timely data processing.
- **Incremental Loading with dbt:** Utilizing dbt's incremental functionality significantly speeds up the workflow and reduces costs by only processing new or changed data instead of reprocessing the entire dataset.
- **Future Improvements:** Given more time, further optimizations can be made in data transformation logic and monitoring.

## Go Further

To further enhance the project, consider the following improvements:
- **Use PySpark or Snowpark:** Replace pandas with PySpark or Snowpark to handle larger datasets more efficiently during data transformations.
- **Data Quality Tests:** Implement data quality tests, both singular and generic, to ensure the accuracy and reliability of the data.
- **Dashboard Creation:** Create a dashboard using Power BI or Tableau that queries the `mart_bitcoin_analysis` table to visualize the results and KPIs.

## Contact

Please feel free to contact me if you have any questions.