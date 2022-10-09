###############################################################
# Author: Konrad Borowiec
# Date: 2022-10-09
###############################################################
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

###############################################################
# Parameters
###############################################################
# Paths
SPARK_MASTER = "spark://spark:7077"
ROOT_PATH_DAG = "/usr/local/airflow/dags/process_web_log"
# CONFIG_PATH = f"{ROOT_PATH_DAG}/config"
# CONFIG_JSON = f"{CONFIG_PATH}/config.json"
TRANSFORM_PATH = f"{ROOT_PATH_DAG}/transform"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"
ACCESS_LOG = f"{INGEST_PATH}/accesslog.txt"
CURATED_PATH = f"{ROOT_PATH_DAG}/curated"
BUSINESS_READY_PATH = f"{ROOT_PATH_DAG}/business_ready"
SCRIPTS_PATH = f"{ROOT_PATH_DAG}/scripts"
BASH_SCRIPTS_PATH = f"{SCRIPTS_PATH}/bash"
PYTHON_SCRIPTS_PATH = f"{SCRIPTS_PATH}/python"

# Load JSON Config
# json_config = json.load(open(CONFIG_JSON, "r"))

# Dates
TODAY = date.today()
YEARS = range(2019, 2023)
DT = '{{ ti.xcom_pull(task_ids="setup_business_dt", key="return_value") }}'
DT_MINUS_ONE = '{{ (execution_date + macros.timedelta(days=-1)).strftime("%Y-%m-%d") }}'
DT_MACRO = '{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d") }}'


#####################################################################################
# Python Functions
#####################################################################################


#####################################################################################
# DAG Definition
#####################################################################################
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 7),
    "email": "dummy_email@emails.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # "tags": TAGS
}

dag = DAG(
    dag_id="process_web_log",
    # description=DESCRIPTION,
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1
)

#####################################################################################
# Tasks
#####################################################################################
# GET DATE TIME OF STARTED DAG
get_start_datetime = BashOperator(
    task_id="t_get_start_datetime",
    bash_command="""
    echo 'DAG START DATETIME: ';
    start_date=$(date)
    echo $start_date;
    """,
    dag=dag
)

extract_data = BashOperator(
    task_id="extract_data",
    bash_command=
    f"""
    ls -lah {INGEST_PATH};
    cut -f1 -d" " {ACCESS_LOG} > {CURATED_PATH}/extracted_data.txt;
    ls -lah {CURATED_PATH};
    """,
    dag=dag
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command=
    f"""
    ls -lah {CURATED_PATH};
    grep -v 198\.46\.149\.143 {CURATED_PATH}/extracted_data.txt > {TRANSFORM_PATH}/transformed_data.txt;
    ls -lah {TRANSFORM_PATH};
    """,
    dag=dag
)

load_data = BashOperator(
    task_id="load_data",
    bash_command=
    f"""
    cd {TRANSFORM_PATH}
    tar -czvf weblog.tar.gz {TRANSFORM_PATH};
    ls -lah {TRANSFORM_PATH};
    mv {TRANSFORM_PATH}/weblog.tar.gz {BUSINESS_READY_PATH}/weblog.tar.gz;
    ls -lah {BUSINESS_READY_PATH};
    """,
    dag=dag
)

# GET DATE TIME OF FINISHED DAG
get_end_datetime = BashOperator(
    task_id="t_get_end_datetime",
    bash_command="""
    echo 'DAG END DATETIME: ';
    end_date=$(date)
    echo $end_date;
    """,
    trigger_rule="one_success",
    dag=dag
)


#####################################################################################
# Set Relations Between Tasks
#####################################################################################
get_start_datetime >> extract_data
extract_data >> transform_data
transform_data >> load_data
load_data >> get_end_datetime
