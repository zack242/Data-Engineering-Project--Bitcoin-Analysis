from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'ingestion_bitcoin_data',
    default_args=default_args,
    description='Ingest Bitcoin data into Snowflake from S3',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    params={"date_of_ingestion": "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"},
) as dag:

    # Task to use the warehouse and database
    setup_snowflake_environment = SnowflakeOperator(
        task_id='setup_snowflake_environment',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE WAREHOUSE blockchain_wh;
        USE DATABASE bitcoin_db;
        USE ROLE user_role;
        
        CREATE SCHEMA IF NOT EXISTS bitcoin_db.bitcoin_sch;
        USE SCHEMA bitcoin_db.bitcoin_sch;  
        
        CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format 
        TYPE = parquet;      
        """,
    )

    # Task to copy data into blocks table
    ingest_blocks_data_from_s3 = SnowflakeOperator(
        task_id='ingest_blocks_data_from_s3',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE ROLE user_role;
        CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage
        FILE_FORMAT = sf_tut_parquet_format
        URL = 's3://aws-public-blockchain/v1.0/btc/';
        
        COPY INTO bitcoin_db.bitcoin_sch.blocks
        FROM (
            SELECT
                $1:date::varchar,
                $1:hash::varchar,
                $1:size::number,
                $1:stripped_size::number,
                $1:weight::number,
                $1:number::number,
                $1:version::number,
                $1:merkle_root::varchar,
                $1:timestamp::varchar,
                $1:nonce::number,
                $1:bits::varchar,
                $1:coinbase_param::varchar,
                $1:transaction_count::number,
                $1:mediantime::varchar,
                $1:difficulty::float,
                $1:chainwork::varchar,
                $1:previousblockhash::varchar,
                CURRENT_TIMESTAMP() AS ingestion_timestamp
            FROM @sf_tut_stage/blocks/date={{ params.date_of_ingestion }}/
        )
        FILE_FORMAT = (TYPE = parquet);
        """,
    )

    # Task to copy data into transactions table
    ingest_transactions_data_from_s3 = SnowflakeOperator(
        task_id='ingest_transactions_data_from_s3',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE ROLE user_role;
        CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage
        
        FILE_FORMAT = sf_tut_parquet_format
        URL = 's3://aws-public-blockchain/v1.0/btc/';
        
        COPY INTO bitcoin_db.bitcoin_sch.transactions
        FROM (
            SELECT
                $1:date::varchar,
                $1:hash::varchar,
                $1:size::number,
                $1:virtual_size::number,
                $1:version::number,
                $1:lock_time::number,
                $1:block_hash::varchar,
                $1:block_number::varchar,
                $1:block_timestamp::varchar,
                $1:index::number,
                $1:input_count::number,
                $1:output_count::number,
                $1:input_value::float,
                $1:output_value::float,
                $1:is_coinbase::boolean,
                $1:fee::float,
                $1:inputs::variant,
                $1:outputs::variant,
                CURRENT_TIMESTAMP() AS ingestion_timestamp
            FROM @sf_tut_stage/transactions/date={{ params.date_of_ingestion }}/
        )
        FILE_FORMAT = (TYPE = parquet);
        """,
    )
    
    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_bitcoin_analysis',
        trigger_dag_id='dbt_bitcoin_analysis',  # Example of passing parameters
        conf={ "cut_off_date": "{{ params.date_of_ingestion }}"},
    )

    # Define task dependencies
    setup_snowflake_environment >> ingest_blocks_data_from_s3 >> ingest_transactions_data_from_s3
    ingest_transactions_data_from_s3 >> trigger_child_dag
