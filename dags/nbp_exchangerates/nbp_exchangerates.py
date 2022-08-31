from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta


###############################################################
# Parameters
###############################################################
SPARK_MASTER = "spark://spark:7077"
ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
TRANSFER_PATH = f"{ROOT_PATH_DAG}/transfer"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"
TRANSFORM_PATH = f"{ROOT_PATH_DAG}/transform"
BUSINESS_READY_PATH = f"{ROOT_PATH_DAG}/business_ready"
SCRIPTS_PATH= f"{ROOT_PATH_DAG}/scritps"
BASH_SCRIPTS_PATH = f"{SCRIPTS_PATH}/bash"
PYTHON_SCRIPTS_PATH = f"{SCRIPTS_PATH}/python"
# CSV_FILE = "/usr/local/spark/resources/data/movies.csv"
TODAY = date.today()
#URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
#ARCHIVE_NAME="tolldata.tgz"


###############################################################
# DAG Definition
###############################################################
default_args = {
    "owner": "dummy_name",
    "depends_on_past": False,
    "start_date": datetime(TODAY.year, TODAY.month, TODAY.day),
    "email": ["dummy_name@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    dag_id="nbp_exchangerates",
    description="Assessment Task",
    schedule_interval="@daily",
    default_args=default_args
)


###############################################################
# Tasks
###############################################################
# GET DATE TIME OF FINISHED DAG
get_start_datetime = BashOperator(
    task_id="t_get_start_datetime",
    bash_command="""
    echo 'DAG START DATETIME: ';
    start_date=$(date)
    echo $start_date;
    """,
    dag=dag
)


# CREATE TRANSFER PATH IN NOT EXISTS
get_transfer_path = BashOperator(
    task_id="t_get_transfer_path",
    bash_command=
    f"""
    echo 'CREATING TRANSFER DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {TRANSFER_PATH} ];
    then
      mkdir -p {TRANSFER_PATH};
    fi;
    """,
    dag=dag
)

# CREATE INGEST PATH IN NOT EXISTS
get_ingest_path = BashOperator(
    task_id="t_get_ingest_path",
    bash_command=
    f"""
    echo 'CREATING INGEST DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {INGEST_PATH} ];
    then
      mkdir -p {INGEST_PATH};
    fi;
    """,
    dag=dag
)


# CREATE TRANSFOR PATH IN NOT EXISTS
get_transform_path = BashOperator(
    task_id="t_get_transform_path",
    bash_command=
    f"""
    echo 'CREATING TRASNFORM DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {TRANSFORM_PATH} ];
    then
      mkdir -p {TRANSFORM_PATH};
    fi;
    """,
    dag=dag
)


# CREATE BUSINESS READY PATH IN NOT EXISTS
get_business_ready_path = BashOperator(
    task_id="t_get_output_path",
    bash_command=
    f"""
    echo 'CREATING BUSINESS READY DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {BUSINESS_READY_PATH} ];
    then
      mkdir -p {BUSINESS_READY_PATH};
    fi;
    """,
    dag=dag
)

# INSTALL LIBRARIES
get_python_libraries = BashOperator(
    task_id="t_get_python_libraries",
    bash_command=
    f"""
    echo 'INSTALLING PYTHON LIBRARIES RELATED TO THE DAG nbp_exchangerates.py: ';
    pip install -r {ROOT_PATH_DAG}/requirements.txt
    """,
    dag=dag
)

# INSTALL LIBRARIES
get_holidays = BashOperator(
    task_id="t_get_holidays",
    bash_command=
    f"""
    echo 'INSTALLING PYTHON LIBRARIES RELATED TO THE DAG nbp_exchangerates.py: ';
    pip install -r {ROOT_PATH_DAG}/requirements.txt
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
    dag=dag
)

###############################################################
# Defining Tasks relations
###############################################################
get_start_datetime >> get_transfer_path
get_transfer_path >> get_ingest_path
get_ingest_path >> get_transform_path
get_transform_path >> get_business_ready_path
get_business_ready_path >> get_python_libraries
get_python_libraries >> get_end_datetime
