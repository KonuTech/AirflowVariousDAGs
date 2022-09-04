###############################################################
# Author: Konrad Borowiec
# Date: 2022-09-02
###############################################################
import os
import time
import json
import requests
import pandas as pd
from dateutil import easter
from dateutil.relativedelta import *
from pandas.io.json import json_normalize
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


###############################################################
# Parameters
###############################################################
# Paths
SPARK_MASTER = "spark://spark:7077"
ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
CONFIG_PATH = f"{ROOT_PATH_DAG}/config"
CONFIG_JSON = f"{CONFIG_PATH}/config.json"
TRANSFER_PATH = f"{ROOT_PATH_DAG}/transfer"
EXCURSIONS = f"{TRANSFER_PATH}/excursions_data.csv"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"
NBP_EXCHANGE_RATES = f"{INGEST_PATH}/nbp_exchangerates.csv"
NBP_EXCHANGE_RATES_LATEST = f"{INGEST_PATH}/nbp_exchangerates_latest.csv"
CURATED_PATH = f"{ROOT_PATH_DAG}/curated"
BUSINESS_READY_PATH = f"{ROOT_PATH_DAG}/business_ready"
SCRIPTS_PATH = f"{ROOT_PATH_DAG}/scripts"
BASH_SCRIPTS_PATH = f"{SCRIPTS_PATH}/bash"
PYTHON_SCRIPTS_PATH = f"{SCRIPTS_PATH}/python"

# Load JSON Config
json_config = json.load(open(CONFIG_JSON, "r"))
URL_NBP_API= json_config["URL_NBP_API"]
OWNER = json_config["OWNER"]
EMAIL = json_config["EMAIL"]
TAGS = json_config["TAGS"]
CURRENCY_CODES = json_config["CURRENCY_CODES"]
DAG_ID = json_config["DAG_ID"]
DESCRIPTION = json_config["DESCRIPTION"]


# Dates
TODAY = date.today()
YEARS = range(2019, 2023)
DT = '{{ ti.xcom_pull(task_ids="setup_business_dt", key="return_value") }}'
DT_MINUS_ONE = '{{ (execution_date + macros.timedelta(days=-1)).strftime("%Y-%m-%d") }}'
DT_MACRO = '{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d") }}'



###############################################################
# Python Functions
###############################################################
# GET BUSINESS DATE FROM AIRFLOW'S EXECUTION DATE
def setup_business_dt(**kwargs):
    """
    :param kwargs:
    :return:
    """
    dt = (kwargs.get("execution_date", None)).strftime('%Y%m%d')
    return dt


# CHECK IF GIVEN DATE IS A WORKING DAY IN POLAND
def check_if_working_day(file, dt):
    """
    :param file:
    :param dt:
    :return:
    """
    # Get a calendar of working days in Poland
    df = pd.read_csv(file)

    # Check if given date in a calendar of working days in Poland
    if dt in sorted(set(df['date'])):
        print("WORKDAY. CONTINUE WITH PROCESS.")
    else:
        raise AirflowException("HOLIDAY. STOPPING PROCESS.")


# CHECK IF GIVEN FILE EXISTS
def check_if_file_exists(file):
    """
    :param file:
    :return:
    """
    # Check if given files exists
    if os.path.exists(file):
        return True
    else:
        raise AirflowException("MISSING INPUT DATA OR OUTPUT SCHEMA")


# GET A CALENDAR OF WORKING DAY IN POLAND
def get_working_days(ingest_path, first_year, last_year, weekmask):
    """
    :param ingest_path:
    :param first_year:
    :param last_year:
    :param weekmask:
    :return:
    """
    # Get a range of years
    years = range(first_year, last_year)

    # Get empty Pandas data frame
    df = pd.DataFrame()

    # Loop over years
    for year in years:

        # Get Polish holidays
        easter_sunday = easter.easter(year)
        holidays = {'New Year': date(year, 1, 1),
                    'Trzech Kroli': date(year, 1, 6),
                    'Easter Sunday': easter_sunday,
                    'Easter Monday': easter_sunday + timedelta(days=1),
                    'Labor Day': date(year, 5, 1),
                    'Constitution Day': date(year, 5, 3),
                    'Pentecost Sunday': easter_sunday + relativedelta(days=+1, weekday=SU(+7)),
                    'Corpus Christi': easter_sunday + relativedelta(weekday=TH(+9)),
                    'Assumption of the Blessed Virgin Mary': date(year, 8, 15),
                    'All Saints\' Day': date(year, 11, 1),
                    'Independence Day': date(year, 11, 11),
                    'Christmas  Day': date(year, 12, 25),
                    'Boxing Day': date(year, 12, 26),
                    }

        # Insert holidays into data frame
        df = pd.concat([df, pd.DataFrame([holidays])], ignore_index=True)
        df.index.names = ['id']

        # Transpose data frame
        df_t = df.transpose(copy=True)

        # Clean data frame
        df_t = df_t.stack().reset_index(name='date').rename(columns={'level_0': 'holiday', 'id': 'year_id'})
        df_t = df_t.drop(columns='year_id')
        df_t['date'] = pd.to_datetime(df_t['date'])

    # Get working days excluding dates for Polish holidays
    working_days = pd.DataFrame(
        pd.bdate_range(
            start=f"1/1/{first_year}",
            end=f"1/1/{last_year}",
            holidays=list(df_t['date']),
            weekmask=weekmask,
            freq='C'),
        columns=['date']
    )

    # Drop duplicates if exist
    working_days.drop_duplicates(inplace=True)

    # Save output as CSV
    working_days.to_csv(f'{ingest_path}/working_days.csv', index=False)


# GET NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY
def get_latest_exchange_rates(ingest_path, file, currency_codes, url_nbp_api, dt, dt_minus_one):
    """
    :param ingest_path:
    :param file:
    :param currency_codes:
    :param url_nbp_api:
    :param dt:
    :param dt_minus_one:
    :return:
    """

    print("\nCurrent dt: ", dt)
    print("\ndt_minus_one: ", dt_minus_one)

    # Get calendar of working days in Poland
    df = pd.read_csv(file, infer_datetime_format=True)
    df['date'] = df['date'].astype('datetime64[ns]')
    print(df.info())
    print(df)

    # Convert DT to datetime
    day_number = pd.to_datetime(dt).weekday()

    if day_number in (0, 5, 6):  # 0 Monday, Saturday, Sunday
        print(
            """Monday, Saturday or Sunday - Get Exchange rates from previous working day.
            Week Day number: """,
            day_number
        )

        # Check if (DT - 1) in a calendar of working days in Poland
        if str(dt_minus_one) in sorted(set(df['date'].astype('str'))):

            # (DT -1) is a Working day
            # Get previous working day in Poland
            print("Previous day is a working day: ", dt_minus_one)

            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP exchange rates data
            for currency_code in currency_codes:
                # if currency_code != 'RUB':
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{dt_minus_one}/{dt_minus_one}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{dt_minus_one}/{dt_minus_one}/").json()['rates']
                    json_norm = json_normalize(respond)
                    json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                    json_norm['exchange_rate'] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace of date
            print("Print current date used to replace date of last working day")
            print(dt)
            print("Print APi output before replace:")
            print(output)
            # Replace API date - here a date of last working day - with DAGs Execution date
            # to allow for correct left join in merge by date and exchange rate type
            print("Print APi output after replace:")
            output.loc[output['effectiveDate'] == str(dt_minus_one), 'effectiveDate'] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f'{ingest_path}/nbp_exchangerates_latest.csv', index=False, header=False)

        else:
            # (DT -1) is not a Working day
            # Get previous working day in Poland
            print("Previous day not a working day: ", dt_minus_one)
            print("Get previous working day in Poland: ")
            print(df[df['date'] < dt_minus_one])
            previous_days = df[df['date'] < dt_minus_one]

            print('Max index value:')
            print(previous_days.loc[previous_days['date'].idxmax()][0])
            previous_working_day = (previous_days.loc[previous_days['date'].idxmax()][0]).strftime("%Y-%m-%d")
            print("previous_working_day: ", previous_working_day)

            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}/").json()['rates']
                    json_norm = json_normalize(respond)
                    json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                    json_norm['exchange_rate'] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace of date
            print("Print current date used to replace date of last working day")
            print(dt)
            print("Print APi output before replace:")
            print(output)
            # Replace API date - here a date of last working day - with DAGs Execution date
            # to allow for correct left join in merge by date and exchange rate type
            print("Print APi output after replace:")
            output.loc[output['effectiveDate'] == str(previous_working_day), 'effectiveDate'] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f'{ingest_path}/nbp_exchangerates_latest.csv', index=False, header=False)

    else:
        print("Weekday- Get Exchange rates from previous working day. Week day number: ", day_number)

        # Check if (DT - 1) in a calendar of Working Days in Poland
        if str(dt_minus_one) in sorted(set(df['date'].astype('str'))):

            # (DT -1) is a Working day
            # Get previous working day in Poland
            print("Previous day is a working day: ", dt_minus_one)

            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{dt_minus_one}/{dt_minus_one}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{dt_minus_one}/{dt_minus_one}/").json()['rates']
                    json_norm = json_normalize(respond)
                    json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                    json_norm['exchange_rate'] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace of date
            print("Print current date used to replace date of last working day")
            print(dt)
            print("Print APi output before replace:")
            print(output)
            # Replace API date - here a date of last working day - with DAGs Execution date
            # to allow for correct left join in merge by date and exchange rate type
            print("Print APi output after replace:")
            output.loc[output['effectiveDate'] == str(dt_minus_one), 'effectiveDate'] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f'{ingest_path}/nbp_exchangerates_latest.csv', index=False, header=False)

        else:
            # (DT -1) is not a Working Day in Poland
            # Get previous working day in Poland
            print("Previous day not a working day: ", dt_minus_one)
            print("Get previous working day in Poland: ")
            print(df[df['date'] < dt_minus_one])
            previous_days = df[df['date'] < dt_minus_one]

            print('Max index value:')
            print(previous_days.loc[previous_days['date'].idxmax()][0])
            previous_working_day = (previous_days.loc[previous_days['date'].idxmax()][0]).strftime("%Y-%m-%d")
            print("previous_working_day: ", previous_working_day)

            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}/").json()['rates']
                    json_norm = json_normalize(respond)
                    json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                    json_norm['exchange_rate'] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace of date
            print("Print current date used to replace date of last working day")
            print(dt)
            print("Print APi output before replace:")
            print(output)
            # Replace API date - here a date of last working day - with DAGs Execution date
            # to allow for correct left join in merge by date and exchange rate type
            print("Print APi output after replace:")
            output.loc[output['effectiveDate'] == str(previous_working_day), 'effectiveDate'] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f'{ingest_path}/nbp_exchangerates_latest.csv', index=False, header=False)


# APPEND NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND
def append_latest_exchange_rate(ingest_path, nbp_exchange_rates_latest):
    """
    :param ingest_path:
    :param nbp_exchange_rates_latest:
    :return:
    """

    # Insert the most recent exchange rates for previous working day
    df = pd.read_csv(nbp_exchange_rates_latest)
    print(df.shape)

    # Drop duplicates if exist
    df.drop_duplicates(inplace=True)

    # Save output as CSV
    df.to_csv(f'{ingest_path}/nbp_exchangerates.csv', mode='a', index=False)


# INSERT NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND TO EXCURSIONS TABLE
def merge_exchange_rates(curated_path, excursions, nbp_exchange_rates):
    """
    :param curated_path:
    :param excursions:
    :param nbp_exchange_rates:
    :return:
    """
    # Get excursions data
    df_1 = pd.read_csv(excursions)
    print(df_1.shape)

    # Get exchange rates data for previous working day
    df_2 = pd.read_csv(nbp_exchange_rates)
    print(df_2.shape)

    # Merge both data sets
    output = df_1.merge(
        df_2,
        how='left',
        left_on=['SP_TourDate', 'SP_PaidCurrency'],
        right_on=['effectiveDate', 'exchange_rate']
    )

    # Drop duplicates if exist
    output.drop_duplicates(inplace=True)

    # Save output to CSV
    output.to_csv(f'{curated_path}/nbp_exchangerates.csv', index=False)


# CALCULATE VALUES FOR PAID EXCURSIONS IN PLN AND SAVE OUTPUT AS AVRO
def calculate_values(curated_path, business_ready_path):
    """
    :param curated_path:
    :param business_ready_path:
    :return:
    """
    # Get merged data
    df_1 = pd.read_csv(f"{curated_path}/nbp_exchangerates.csv")
    print(df_1.shape)

    # Clean data
    df_1.drop(columns=['no', 'effectiveDate', 'exchange_rate'], inplace=True)
    df_1['SP_Paid'] = df_1['SP_Paid'].str.replace(',','.')
    df_1['SP_ValueCalculated'] = (df_1['SP_Paid']).apply(float) * df_1['mid'].apply(float)
    df_1.rename(columns={"mid": "SP_ExchangeRate"}, inplace=True)

    # Reorder columns
    new_cols = [col for col in df_1.columns if col != 'SP_ExchangeRate'] + ['SP_ExchangeRate']
    output = df_1[new_cols]

    # Drop duplicates if exist
    output.drop_duplicates(inplace=True)

    # Save output as CSV
    output.to_csv(f'{business_ready_path}/nbp_exchangerates.csv', index=False)


###############################################################
# DAG Definition
###############################################################
default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    # "start_date": datetime(2022, 8, 1),
    "start_date": datetime(2019, 6, 1),
    "email": EMAIL,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": TAGS
}

dag = DAG(
    dag_id=DAG_ID,
    description=DESCRIPTION,
    # schedule_interval="0 1 * * 1-5",
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1
)


###############################################################
# Tasks
###############################################################
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


# CREATE CURATED PATH IN NOT EXISTS
get_curated_path = BashOperator(
    task_id="t_get_curated_path",
    bash_command=
    f"""
    echo 'CREATING CURATED DIRECTORY IF NOT EXISTS: ';
    if [ ! -d {CURATED_PATH} ];
    then
      mkdir -p {CURATED_PATH};
    fi;
    """,
    dag=dag
)


# CREATE BUSINESS READY PATH IN NOT EXISTS
get_business_ready_path = BashOperator(
    task_id="t_get_business_ready_path",
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


# GET BUSINESS DATE FROM AIRFLOW'S EXECUTION DATE
get_setup_business_dt = PythonOperator(
    task_id="t_get_setup_business_dt",
    provide_context=True,
    python_callable=setup_business_dt,
    dag=dag
)


# GET A CALENDAR OF WORKING DAY IN POLAND
get_working_days = PythonOperator(
    task_id="t_get_working_days",
    python_callable=get_working_days,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        'first_year': 2019,
        'last_year': 2023,
        'weekmask': "Mon Tue Wed Thu Fri"
    },
    dag=dag
)


# CHECK IF EXCURSIONS FILE EXISTS
check_if_excursions_exists = PythonOperator(
    task_id="t_check_if_excursions_exists",
    provide_context=False,
    python_callable=check_if_file_exists,
    op_kwargs={
        "file": EXCURSIONS
    },
    dag=dag
)


# CHECK IF NBP EXCHANGE RATES FILE EXIST
check_if_nbp_exchange_rates_ingest_schema_exists = PythonOperator(
    task_id="t_check_if_nbp_exchange_rates_ingest_schema_exists",
    provide_context=False,
    python_callable=check_if_file_exists,
    op_kwargs={
        "file": NBP_EXCHANGE_RATES
    },
    dag=dag
)


# CHECK IF ONLY ONE TASK FAILED
check_one_failed = DummyOperator(
    task_id="t_check_one_failed",
    trigger_rule='one_failed',
    dag=dag
)


# CHECK IF ALL TASKS ARE SUCCEDED
check_all_success = DummyOperator(
    task_id="t_check_all_success",
    trigger_rule='all_success',
    dag=dag
)


# GET NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY
get_latest_exchange_rates = PythonOperator(
    task_id="t_get_latest_exchange_rates",
    python_callable=get_latest_exchange_rates,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        "file": f"{INGEST_PATH}/working_days.csv",
        'currency_codes': CURRENCY_CODES,
        'url_nbp_api': URL_NBP_API,
        'dt': DT_MACRO,
        'dt_minus_one': DT_MINUS_ONE
    },
    trigger_rule='one_success',
    dag=dag
)


# APPEND NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND
append_latest_exchange_rate = PythonOperator(
    task_id="t_append_latest_exchange_rate",
    python_callable=append_latest_exchange_rate,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        'nbp_exchange_rates_latest': NBP_EXCHANGE_RATES_LATEST
    },
    trigger_rule='one_success',
    dag=dag
)


# MERGE NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND AS DT (CURRENT EXECUTION DATE) TO DATE OF EXCURSIONS TABLE
# IN OTHER WORDS: A MERGE IS DONE BY EXCHANGE RATE AND PREVIOUS WORKING DATE REPLACED WITH CURRENT DATE
merge_exchange_rates = PythonOperator(
    task_id="t_merge_exchange_rates",
    python_callable=merge_exchange_rates,
    op_kwargs={
        'curated_path': CURATED_PATH,
        'excursions': EXCURSIONS,
        'nbp_exchange_rates': NBP_EXCHANGE_RATES
    },
    trigger_rule='one_success',
    dag=dag
)


# CALCULATE VALUES FOR PAID EXCURSIONS IN PLN
calculate_values = PythonOperator(
    task_id="t_calculate_values",
    python_callable=calculate_values,
    op_kwargs={
        'curated_path': CURATED_PATH,
        'business_ready_path': BUSINESS_READY_PATH
    },
    trigger_rule='one_success',
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
    trigger_rule='one_success',
    dag=dag
)


###############################################################
# Set Relations Between Tasks
###############################################################
get_start_datetime >> [get_transfer_path, get_ingest_path, get_business_ready_path, get_curated_path]
[get_transfer_path, get_ingest_path, get_business_ready_path, get_curated_path] >> get_setup_business_dt
get_setup_business_dt >> get_working_days

get_working_days >> [check_if_excursions_exists, check_if_nbp_exchange_rates_ingest_schema_exists]
[check_if_excursions_exists, check_if_nbp_exchange_rates_ingest_schema_exists] >> check_one_failed
[check_if_excursions_exists, check_if_nbp_exchange_rates_ingest_schema_exists] >> check_all_success

check_one_failed >> get_end_datetime
check_all_success >> get_latest_exchange_rates

get_latest_exchange_rates >> append_latest_exchange_rate
append_latest_exchange_rate >> merge_exchange_rates
merge_exchange_rates >> calculate_values
calculate_values >> get_end_datetime
