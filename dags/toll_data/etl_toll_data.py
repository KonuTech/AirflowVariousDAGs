# TO DO: further parametrization
# TO DO: remove intermediary data
# TO DO: exit codes


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import date, datetime, timedelta


###############################################
# Parameters
###############################################
SPARK_MASTER = "spark://spark:7077"
INGEST_PATH = "/usr/local/airflow/ingest"
OUTPUT_PATH = "/usr/local/airflow/output"
STAGING_PATH = "/usr/local/airflow/finalassignment/staging"
SCRIPTS_PATH= "/usr/local/airflow/dags/scripts"
BASH_SCRIPTS_PATH = f"{SCRIPTS_PATH}/bash"
# CSV_FILE = "/usr/local/spark/resources/data/movies.csv"
TODAY = date.today()
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
ARCHIVE_NAME="tolldata.tgz"


###############################################
# DAG Definition
###############################################
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
        dag_id="etl_toll_data",
        description="Peer-graded-Assignment",
        schedule_interval="@daily",
        default_args=default_args
    )


###############################################
# Tasks
###############################################
# GET DAG START DATE
get_start_date = BashOperator(
    task_id="t_get_start_date",
    bash_command="""
    echo 'DAG START DATE: ';
    start_date=$(date)
    echo $start_date;
    """,
    dag=dag
)


# CHECK IF INGEST PATH EXISTS
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


# CHECK IF OUTPUT PATH EXISTS
get_output_path = BashOperator(
    task_id="t_get_output_path",
    bash_command=
    f"""
    echo 'CREATING OUTPUT DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {OUTPUT_PATH} ];
    then
      mkdir -p {OUTPUT_PATH};
    fi;
    """,
    dag=dag
)


# CHECK IF STAGING PATH EXISTS
get_staging_path = BashOperator(
    task_id="t_get_staging_path",
    bash_command=
    f"""
    echo 'CREATING STAGING DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {STAGING_PATH} ];
    then
      mkdir -p {STAGING_PATH};
    fi;
    """,
    dag=dag
)


# CHECK IF STATUS 200
get_status_code = BashOperator(
    task_id="t_status_code",
    env={
        'URL': URL
    },
    bash_command=f"{BASH_SCRIPTS_PATH}/get_status_code.sh ", # Works with a space at the end
    dag=dag
)


# DOWNLOAD DATA
get_external_data = BashOperator(
    task_id="t_get_external_data",
    bash_command=
    f"""
    echo 'DOWNLOADING ARCHIVED DATA PACKAGE: {URL}';
    wget '{URL}' -P {INGEST_PATH} -O {INGEST_PATH}/{ARCHIVE_NAME};
    """,
    dag=dag
)


# EXTRACT DATA
get_extracted_data = BashOperator(
    # task_id="t_get_extracted_data",
    task_id="unzip_data",
    bash_command=
    f"""
    if [ ! -f {INGEST_PATH}/{ARCHIVE_NAME} ];
    then
        echo 'MISSING ACHIVED DATA: {ARCHIVE_NAME}'
    else
        echo 'EXTRACTING ARCHIVED DATA: {ARCHIVE_NAME}'
        tar -zxvf {INGEST_PATH}/{ARCHIVE_NAME} -C {INGEST_PATH}
    fi
    """,
    dag=dag
)


# GET CSV OUTPUT
get_csv_output = BashOperator(
    # task_id="t_get_csv_output",
    task_id="extract_data_from_csv",
    env={
        "ingest_path": INGEST_PATH,
        "output_path": OUTPUT_PATH,
        "input": "vehicle-data.csv",
        "output": "csv_data.csv"
    },
    bash_command=f"{BASH_SCRIPTS_PATH}/get_csv_output.sh ",
    dag=dag
)


# GET TSV OUTPUT
get_tsv_output = BashOperator(
    # task_id="t_get_tsv_output",
    task_id="extract_data_from_tsv",
    env={
        "ingest_path": INGEST_PATH,
        "output_path": OUTPUT_PATH,
        "input": "tollplaza-data.tsv",
        "output": "tsv_data.csv"
    },
    bash_command=f"{BASH_SCRIPTS_PATH}/get_tsv_output.sh ",
    dag=dag
)


# GET TXT OUTPUT
get_txt_output = BashOperator(
    # task_id="t_get_txt_output",
    task_id="extract_data_from_fixed_width",
    env={
        "ingest_path": INGEST_PATH,
        "output_path": OUTPUT_PATH,
        "input": "payment-data.txt",
        "output": "fixed_width_data.csv"
    },
    bash_command=f"{BASH_SCRIPTS_PATH}/get_txt_output.sh ",
    dag=dag
)


# GET JOINED DATA
get_joined_data = BashOperator(
    # task_id="t_get_joined_data,
    task_id="consolidate_data",
    bash_command=f"""
    paste -d ',' {OUTPUT_PATH}/csv_data.csv {OUTPUT_PATH}/tsv_data.csv {OUTPUT_PATH}/fixed_width_data.csv > {OUTPUT_PATH}/extracted_data.csv
    """,
    dag=dag
)


# GET TRANSFORMED DATA
get_transformed_data = BashOperator(
    # task_id="t_get_transformed_data",
    task_id="transform_data",
    env={
        "input_path": OUTPUT_PATH,
        "output_path": STAGING_PATH,
        "input": "extracted_data.csv",
        "output": "transformed_data.csv"
    },
    bash_command=f"{BASH_SCRIPTS_PATH}/get_transformed_data.sh ",
    dag=dag
)


# GET DAG END DATE
get_end_date = BashOperator(
    task_id="t_get_end_date",
    bash_command="""
    echo 'DAG END DATE: ';
    end_date=$(date)
    echo $end_date;
    """,
    dag=dag
)


get_start_date >> get_ingest_path
get_ingest_path >> get_staging_path
get_staging_path >> get_output_path
get_output_path >> get_status_code
get_status_code >> get_external_data
get_external_data >> get_extracted_data
get_extracted_data >> get_csv_output
get_csv_output >> get_tsv_output
get_tsv_output >> get_txt_output
get_txt_output >> get_joined_data
get_joined_data >> get_transformed_data
get_transformed_data >> get_end_date

