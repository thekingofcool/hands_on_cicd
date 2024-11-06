# data_processing_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

# 定义默认参数
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# 创建 DAG
dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='A simple data processing DAG',
    schedule_interval='@daily',
)

# 定义执行 Spark 脚本的函数
def run_spark_job():
    subprocess.run(["spark-submit", "process_data.py"])

# 创建任务
run_spark = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag,
)

# 设置任务依赖关系
run_spark
