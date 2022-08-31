###############################################################
# Author: Konrad Borowiec
###############################################################

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
import dateutil.rrule as rrule
from dateutil import easter
import pandas as pd
from pandas.io.json import json_normalize
import requests
import time

###############################################################
# Parameters
###############################################################
SPARK_MASTER = "spark://spark:7077"
ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
TRANSFER_PATH = f"{ROOT_PATH_DAG}/transfer"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"
CURATED_PATH = f"{ROOT_PATH_DAG}/curated"
BUSINESS_READY_PATH = f"{ROOT_PATH_DAG}/business_ready"
SCRIPTS_PATH = f"{ROOT_PATH_DAG}/scritps"
BASH_SCRIPTS_PATH = f"{SCRIPTS_PATH}/bash"
PYTHON_SCRIPTS_PATH = f"{SCRIPTS_PATH}/python"

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
YEARS = range(2010, 2030)

CURRENCY_CODES = ['CZK', 'EUR', 'GBP', 'HUF', 'RUB', 'USD']

###############################################################
# Python Functions
###############################################################


# Get dated for Polish holidays
def get_holidays_pl(ingest_path, years):
    """
    :param years:
    :param ingest_path:
    :return:
    """

    df = pd.DataFrame()

    for year in years:

        # Get holidays
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

        # Get output
        df = pd.concat([df, pd.DataFrame([holidays])], ignore_index=True)
        df.index.names = ['id']
        output = df.transpose(copy=True)
        output = output.stack().reset_index(name='date').rename(columns={'level_0': 'holiday', 'id': 'year_id'})
        output = output.drop(columns='year_id')
        output['date'] = pd.to_datetime(output['date'])

    output.to_csv(f'{ingest_path}/holidays_pl.csv', index=False)


def get_sundays(ingest_path, years):
    """
    :param years:
    :param ingest_path:
    :return:
    """

    before = datetime(min(years), 1, 1)
    after = datetime(max(years), 12, 31)
    rr = rrule.rrule(rrule.WEEKLY, byweekday=SU, dtstart=before)

    output = pd.DataFrame(rr.between(before, after, inc=True), columns=['date'])
    output['holiday'] = 'Sunday'

    output.to_csv(f'{ingest_path}/sundays.csv', index=False)


def get_nbp_rates(ingest_path, years, yesterday, currency_codes):
    """
    :param ingest_path:
    :param years:
    :param yesterday:
    :param currency_codes:
    :return:
    """

    output = pd.DataFrame()
    for year in years:
        for currency_code in currency_codes:
            print(currency_code)
            print(year)
            try:
                print(f"http://api.nbp.pl/api/exchangerates/rates/a/{currency_code}/{year}-01-01/{year}-12-31/")
                respond = requests.get(f"http://api.nbp.pl/api/exchangerates/rates/a/{currency_code}/{year}-01-01/{year}-12-31/").json()['rates']
                # json_norm = pd.json_normalize(respond)
                json_norm = json_normalize(respond)
                json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                json_norm['exchange_rate'] = currency_code
                print(json_norm)
                output = pd.concat([output, json_norm], ignore_index=True)
                time.sleep(60)
            except Exception:
                print(f"http://api.nbp.pl/api/exchangerates/rates/a/{currency_code}/{year}-01-01/{yesterday}/")
                respond = requests.get(f"http://api.nbp.pl/api/exchangerates/rates/a/{currency_code}/{year}-01-01/{yesterday}/").json()['rates']
                # json_norm = pd.json_normalize(respond)
                json_norm = json_normalize(respond)
                json_norm['effectiveDate'] = pd.to_datetime(json_norm['effectiveDate'])
                json_norm['exchange_rate'] = currency_code
                print(json_norm)
                output = pd.concat([output, json_norm], ignore_index=True)
                time.sleep(60)

    output.to_csv(f'{ingest_path}/nbp_exchangerates.csv', index=False)


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

# INSTALL MISSING PYTHON LIBRARIES
get_python_libraries = BashOperator(
    task_id="t_get_python_libraries",
    bash_command=
    f"""
    echo 'INSTALLING PYTHON LIBRARIES RELATED TO THE DAG nbp_exchangerates.py: ';
    pip install -r {ROOT_PATH_DAG}/requirements.txt
    """,
    dag=dag
)


# GET POLISH HOLIDAYS
get_holidays_pl = PythonOperator(
    task_id="t_get_holidays_pl",
    python_callable=get_holidays_pl,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        'years': YEARS
    },
    dag=dag
)


# GET SUNDAYS
get_sundays = PythonOperator(
    task_id="t_get_sundays",
    python_callable=get_sundays,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        'years': YEARS
    },
    dag=dag
)


# GET NBP RATES
get_nbp_rates = PythonOperator(
    task_id="t_get_nbp_rates",
    python_callable=get_nbp_rates,
    op_kwargs={
        'ingest_path': INGEST_PATH,
        'years': YEARS,
        'yesterday': YESTERDAY,
        'currency_codes': CURRENCY_CODES
    },
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
get_ingest_path >> get_curated_path
get_curated_path >> get_business_ready_path
get_business_ready_path >> get_python_libraries
get_python_libraries >> get_holidays_pl
get_holidays_pl >> get_sundays
get_sundays >> get_nbp_rates
get_nbp_rates >> get_end_datetime
