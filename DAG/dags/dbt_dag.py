import os
from datetime import datetime
from airflow.models.param import Param

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "bitcoin_db", "schema": "bitcoin_sch"},
    ),
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/bitcoin_analysis",
    ),
    operator_args={
        "install_deps": True,
        "vars": {"cut_off_date": "{{ dag_run.conf['cut_off_date']  }}"},
    },
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_bitcoin_analysis",
    params={
        "cut_off_date": Param(default="2024-01-01", type="string"),
    },
)
