import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_KWARGS_DOCKER_OPERATOR = dict(
  image="wsl-spark-apps:1.0.0",
  network_mode="weblogs_lake_network",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  tty=False,
)

COMMON_SPARK_VARS = dict(
  S3_ENDPOINT = os.getenv("S3_ENDPOINT"),
  NESSIE_URI = os.getenv("NESSIE_URI"),
  AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID"),
  AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY"),
  AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION"),
  AWS_REGION = os.getenv("AWS_REGION"),
  
)


default_args ={
  "owner": "airflow",
  "email_on_failure": False,
  "email_on_retry": False,
  "email": "marco_aurelio_reis@yahoo.com.br",
  "retries": 1,
  "retry_delay": timedelta(minutes=5) 
}

with DAG(
  f"dag_eventual_wsl",
  start_date=datetime(year=2024,month=7,day=20,hour=2),
  schedule_interval="@once",
  default_args=default_args,
  max_active_runs=2,
  catchup=False
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )

    create_namespace_bronze = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_namespace",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 1_namespaces/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          NAMESPACE_NAME = "nessie.bronze")
    )

    create_table_bronze_wsl = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_table_bronze_wsl",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 2_table_bronze/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          TABLE_NAME = "nessie.bronze.wsl_logs",
          TABLE_PATH = "s3a://lakehouse/bronze/wsl_logs")
    )

    create_namespace_silver = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_namespace_silver",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 1_namespaces/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          NAMESPACE_NAME = "nessie.silver")
    )

    create_table_silver_wsl = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_table_silver_wsl",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 3_table_silver/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          TABLE_NAME = "nessie.silver.wsl_logs",
          TABLE_PATH = "s3a://lakehouse/silver/wsl_logs")
    )


    create_namespace_gold = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_namespace_gold",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 1_namespaces/pyspark_app.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          NAMESPACE_NAME = "nessie.gold"
      )
    )


    create_view_gold_1 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_top_10_client_ips",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_1.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs",
          GOLD_VIEW_NAME = "nessie.gold.top_10_client_ips",
      )
    )

    create_view_gold_2 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_top_6_endpoints",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_2.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs",
          GOLD_VIEW_NAME = "nessie.gold.top_6_endpoints",
      )
    )

    create_view_gold_3 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_distinct_client_ips",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_3.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs",
          GOLD_VIEW_NAME = "nessie.gold.distinct_client_ips",
      )
    )

    create_view_gold_4 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_log_days_represented",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_4.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs",
          GOLD_VIEW_NAME = "nessie.gold.log_days_represented",
      )
    )


    create_view_gold_5 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_views_gold_aggregations_payload",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_5.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs"
      )
    )

    create_view_gold_6 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      task_id="create_view_gold_day_with_most_client_errors",
      entrypoint="sh /app/0_ddls_commands/eventual_jobs/submit-ddl.sh 4_views_gold/create_view_6.py",
      environment= dict(
          **COMMON_SPARK_VARS,
          SILVER_TABLE_NAME = "nessie.silver.wsl_logs",
          GOLD_VIEW_NAME = "nessie.gold.day_with_most_client_errors",
      )
    )

    starting_process >> create_namespace_bronze >> create_table_bronze_wsl
    starting_process >> create_namespace_silver >> create_table_silver_wsl

    starting_process >> create_namespace_gold >> [create_view_gold_1, create_view_gold_2, create_view_gold_3, create_view_gold_4, create_view_gold_5, create_view_gold_6]