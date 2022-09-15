import csv
import itertools
import os
import time
import json
from logging import getLogger
from pathlib import Path

# import logging
import requests
import pandas as pd
import pandavro as pdx
from dateutil import easter
from dateutil import rrule
from dateutil.relativedelta import *
from pandas.io.json import json_normalize
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
import typing as t

# env variables?
SPARK_MASTER = "spark://spark:7077"
ROOT_PATH_DAG = Path(os.environ.get("ROOT_PATH", Path(__file__).parent))
CONFIG_JSON =  ROOT_PATH_DAG / "config" / "config.json"
TRANSFER_PATH = ROOT_PATH_DAG / "transfer"
EXCURSIONS =  TRANSFER_PATH / "excursions_data.csv"
INGEST_PATH = ROOT_PATH_DAG / "ingest"
NBP_EXCHANGE_RATES = INGEST_PATH  / "nbp_exchangerates.csv"
NBP_EXCHANGE_RATES_LATEST = INGEST_PATH / "nbp_exchangerates_latest.csv"
CURATED_PATH = ROOT_PATH_DAG / "curated"
BUSINESS_READY_PATH = ROOT_PATH_DAG / "business_ready"

logger = getLogger(__name__)

class JsonConfig(t.TypedDict):
    URL_NBP_API: str
    OWNER: str
    EMAIL: str
    TAGS: t.Sequence[str]
    CURRENCY_CODES: t.Sequence[str]
    DAG_ID: str
    DESCRIPTION: str


# to jest bardzo złe. side effect przy importowanie modułu.
with open(CONFIG_JSON) as f:
    config: JsonConfig = json.load(f)

# po co?
# TODAY = date.today()
# nigdzie nie było użyte
#YEARS = range(2019, 2023)
DT = '{{ ti.xcom_pull(task_ids="setup_business_dt", key="return_value") }}'


def setup_business_dt(execution_date: datetime, **kwargs) -> str:
    return execution_date.strftime("%Y%m%d")


# ensure_file_exists, .check_if_file_exists
def check_if_file_exists(file: str) -> bool:
    if os.path.exists(file):
        return True
    else:
        raise AirflowException("MISSING INPUT DATA OR OUTPUT SCHEMA")


def _holiday_for_year(year: int) -> t.Iterator[date]:
    yield date(year, 1, 1)  # New Year
    yield date(year, 1, 6)  # Trzech Kroli

    yield date(year, 5, 1)  # Labor Day
    yield date(year, 5, 3)  # Constitution Day

    easter_sunday = easter.easter(year)
    yield easter_sunday
    yield easter_sunday + timedelta(days=1)  # Easter Monday
    yield easter_sunday + relativedelta(days=+1, weekday=SU(+7))
    yield easter_sunday + relativedelta(weekday=TH(+9)) # Corpus Christi
    yield date(year, 11, 1)  # All Saints' Day
    yield date(year, 11, 11)  # Independence Day
    yield date(year, 12, 25)  # Christmas Day
    yield date(year, 12, 26)  # Boxing Day


def get_working_days(ingest_path: Path, first_year: int, last_year: int, weekmask: t.Sequence[rrule.weekday]):
    start_date = date(first_year, 1, 1)
    end_date = date(last_year, 12, 31)
    all_days = set(dt.date() for dt in rrule.rrule(rrule.DAILY, start_date, until=end_date, byweekday=weekmask))
    holidays = set(itertools.chain.from_iterable(_holiday_for_year(year) for year in range(first_year, last_year + 1)))

    working_days = list(sorted(all_days - holidays))
    # zapisywanie powino być poza tą funkcją
    with open(ingest_path / "working_days.csv", 'w') as f:
        writer = csv.writer(f)
        writer.writerows(map(lambda cell: [cell], ("date", *(f"{day:%Y-%m-%d}" for day in working_days))))

def _is_monday_or_weekend(tested_date: date) -> bool:
    return tested_date.weekday() in (0, 5, 6)

def get_latest_exchange_rates(ingest_path: Path, working_days_path: Path, currency_codes: t.Sequence[str], url_nbp_api: str, business_date: str, business_date_minus_one: str):
    # Print DT (execution date) and DT - 1 to log
    logger.info("Current dt: %s", business_date)
    logger.info("business_date_minus_one: %s", business_date_minus_one)

    # Get custom calendar of working days in Poland
    df = pd.read_csv(working_days_path, infer_datetime_format=True)
    df["date"] = df["date"].astype("datetime64[ns]")

    # Get week day number from DT (execution date)
    business_date_as_date: date = pd.to_datetime(business_date)

    # Process weekends and Mondays
    if _is_monday_or_weekend(business_date_as_date):
        logger.info("Monday, Saturday or Sunday - Get Exchange rates from previous working day. \nWeek Day number: %s", business_date_as_date.weekday())

        working_days_in_poland = sorted(set(df["date"].astype("str")))
        if str§(business_date_minus_one) in working_days_in_poland:
            # TODO: kontynuować
            # (DT -1) is a working day, get previous working day in Poland
            print("\nPrevious day is a working day: ", business_date_minus_one)

            # Sleep to not overload NBP's API with queries
            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP's exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{business_date_minus_one}/{business_date_minus_one}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{business_date_minus_one}/{business_date_minus_one}/").json()[
                        "rates"]
                    json_norm = json_normalize(respond)
                    json_norm["effectiveDate"] = pd.to_datetime(json_norm["effectiveDate"])
                    json_norm["exchange_rate"] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace a date of previous working day with dt (execution date)
            print("\nPrint current date used to replace date of last working day")
            print(business_date)
            print("\nPrint APi output before replace:")
            print(output)
            print("\nPrint APi output after replace:")
            output.loc[output["effectiveDate"] == str(business_date_minus_one), "effectiveDate"] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f"{ingest_path}/nbp_exchangerates_latest.csv", index=False, header=False)

        else:
            # (DT -1) is not a working day, get previous working day in Poland
            print("\nPrevious day not a working day: ", business_date_minus_one)
            print("\nGet previous working day in Poland: ")
            print(df[df["date"] < business_date_minus_one])
            previous_days = df[df["date"] < business_date_minus_one]

            print("\nMax index value:")
            print(previous_days.loc[previous_days["date"].idxmax()][0])
            previous_working_day = (previous_days.loc[previous_days["date"].idxmax()][0]).strftime("%Y-%m-%d")
            print("\nprevious_working_day: ", previous_working_day)

            # Sleep to not overload NBP's API with queries
            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP's exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}")
                    respond = requests.get(
                        f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}/").json()["rates"]
                    json_norm = json_normalize(respond)
                    json_norm["effectiveDate"] = pd.to_datetime(json_norm["effectiveDate"])
                    json_norm["exchange_rate"] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace a date of previous working day with dt (execution date)
            print("\nPrint current date used to replace date of last working day")
            print(dt)
            print("\nPrint APi output before replace:")
            print(output)
            print("\nPrint APi output after replace:")
            output.loc[output["effectiveDate"] == str(previous_working_day), "effectiveDate"] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f"{ingest_path}/nbp_exchangerates_latest.csv", index=False, header=False)

    else:
        print("\nWeekday- Get Exchange rates from previous working day. Week day number: ", day_number)

        # Check if (DT - 1) in a calendar of Working Days in Poland
        if str(business_date_minus_one) in sorted(set(df["date"].astype("str"))):

            # (DT -1) is a working day, get previous working day in Poland
            print("\nPrevious day is a working day: ", business_date_minus_one)

            # Sleep to not overload NBP's API with queries
            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP's exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{business_date_minus_one}/{business_date_minus_one}")
                    respond = requests.get(f"{url_nbp_api}/{currency_code}/{business_date_minus_one}/{business_date_minus_one}/").json()[
                        "rates"]
                    json_norm = json_normalize(respond)
                    json_norm["effectiveDate"] = pd.to_datetime(json_norm["effectiveDate"])
                    json_norm["exchange_rate"] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace a date of previous working day with dt (execution date)
            print("\nPrint current date used to replace date of last working day")
            print(dt)
            print("\nPrint APi output before replace:")
            print(output)
            print("\nPrint APi output after replace:")
            output.loc[output["effectiveDate"] == str(business_date_minus_one), "effectiveDate"] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f"{ingest_path}/nbp_exchangerates_latest.csv", index=False, header=False)

        else:
            # (DT -1) is not a working day, get previous working day in Poland
            print("\nPrevious day not a working day: ", business_date_minus_one)
            print("\nGet previous working day in Poland: ")
            print(df[df["date"] < business_date_minus_one])
            previous_days = df[df["date"] < business_date_minus_one]

            print("\nMax index value:")
            print(previous_days.loc[previous_days["date"].idxmax()][0])
            previous_working_day = (previous_days.loc[previous_days["date"].idxmax()][0]).strftime("%Y-%m-%d")
            print("\nprevious_working_day: ", previous_working_day)

            # Sleep to not overload NBP's API with queries
            time.sleep(3)
            output = pd.DataFrame()

            # Get NBP's exchange rates data
            for currency_code in currency_codes:
                print(currency_code)
                try:
                    print(f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}")
                    respond = requests.get(
                        f"{url_nbp_api}/{currency_code}/{previous_working_day}/{previous_working_day}/").json()["rates"]
                    json_norm = json_normalize(respond)
                    json_norm["effectiveDate"] = pd.to_datetime(json_norm["effectiveDate"])
                    json_norm["exchange_rate"] = currency_code
                    print(json_norm)
                    output = pd.concat([output, json_norm], ignore_index=True)
                except Exception:
                    pass

            # Drop duplicates if exist
            output.drop_duplicates(inplace=True)

            # Replace a date of previous working day with dt (execution date)
            print("\nPrint current date used to replace date of last working day")
            print(dt)
            print("\nPrint APi output before replace:")
            print(output)
            print("\nPrint APi output after replace:")
            output.loc[output["effectiveDate"] == str(previous_working_day), "effectiveDate"] = str(dt)
            print(output)

            # Save output as CSV
            output.to_csv(f"{ingest_path}/nbp_exchangerates_latest.csv", index=False, header=False)


def append_latest_exchange_rate(ingest_path, nbp_exchange_rates_latest):
    """APPEND NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND
    :param ingest_path: path where exchange rates schema is stored along with new, daily inputs
    :param nbp_exchange_rates_latest: path where the latest daily exchange rates are saved
    :return: None
    """

    # Insert the most recent exchange rates for previous working day
    df = pd.read_csv(nbp_exchange_rates_latest)
    print(df.shape)

    # Drop duplicates if exist
    df.drop_duplicates(inplace=True)

    # Save output as CSV
    df.to_csv(f"{ingest_path}/nbp_exchangerates.csv", mode="a", index=False)


def merge_exchange_rates(curated_path, excursions, nbp_exchange_rates):
    """INSERT NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND TO EXCURSIONS TABLE
    :param curated_path: path where merged data is stored
    :param excursions: path where excursion input is stored
    :param nbp_exchange_rates: path where exchange rates schema is stored along with new, daily inputs
    :return: None
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
        how="left",
        left_on=["SP_TourDate", "SP_PaidCurrency"],
        right_on=["effectiveDate", "exchange_rate"]
    )

    # Drop duplicates if exist
    output.drop_duplicates(inplace=True)

    # Save output to CSV
    output.to_csv(f"{curated_path}/nbp_exchangerates.csv", index=False)


def calculate_values(curated_path, business_ready_path):
    """CALCULATE VALUES FOR PAID EXCURSIONS IN PLN
    :param curated_path: path where merged data is stored
    :param business_ready_path:  path where business ready data is stored
    :return:
    """
    # Get merged data
    df_1 = pd.read_csv(f"{curated_path}/nbp_exchangerates.csv")
    print(df_1.shape)

    # Clean data
    df_1.drop(columns=["no", "effectiveDate", "exchange_rate"], inplace=True)
    df_1["SP_Paid"] = df_1["SP_Paid"].str.replace(",", ".")
    df_1["SP_ValueCalculated"] = (df_1["SP_Paid"]).apply(float) * df_1["mid"].apply(float)
    df_1.rename(columns={"mid": "SP_ExchangeRate"}, inplace=True)

    # Reorder columns
    new_cols = [col for col in df_1.columns if col != "SP_ExchangeRate"] + ["SP_ExchangeRate"]
    output = df_1[new_cols]

    # Drop duplicates if exist
    output.drop_duplicates(inplace=True)

    # Save output as CSV
    output.to_csv(f"{business_ready_path}/nbp_exchangerates.csv", index=False)


def get_avro_output(business_ready_path):
    """SAVE BUSINESS READY OUTPUT AS AVRO
    :param business_ready_path: path where business ready data is stored
    :return: None
    """

    # Read CSV business ready file as Pandas data frame
    df = pd.read_csv(f"{business_ready_path}/nbp_exchangerates.csv")
    print(df.shape)

    # Save Pandas data frame as Avro
    pdx.to_avro(f"{business_ready_path}/nbp_exchangerates.avro", df)


#####################################################################################
# DAG Definition
#####################################################################################
default_args = {
    "owner": config["OWNER"],
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 1),
    "email": config["EMAIL"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": config["TAGS"]
}

dag = DAG(
    dag_id=config["DAG_ID"],
    description=config["DESCRIPTION"],
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

# GET BUSINESS DATE FROM EXECUTION DATE
get_setup_business_dt = PythonOperator(
    task_id="t_get_setup_business_dt",
    provide_context=True,
    python_callable=setup_business_dt,
    dag=dag
)

# GET A CUSTOM CALENDAR OF WORKING DAYS IN POLAND
get_working_days = PythonOperator(
    task_id="t_get_working_days",
    python_callable=get_working_days,
    op_kwargs={
        "ingest_path": INGEST_PATH,
        "first_year": 2019,
        "last_year": 2023,
        "weekmask": [rrule.MO.weekday, rrule.TU.weekday, rrule.WE.weekday, rrule.TH.weekday, rrule.FR.weekday]
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

# CHECK IF NBP EXCHANGE RATES FILE EXISTS
check_if_nbp_exchange_rates_ingest_schema_exists = PythonOperator(
    task_id="t_check_if_nbp_exchange_rates_ingest_schema_exists",
    provide_context=False,
    python_callable=check_if_file_exists,
    op_kwargs={
        "file": NBP_EXCHANGE_RATES
    },
    dag=dag
)

check_one_failed = DummyOperator(
    task_id="t_check_one_failed",
    trigger_rule="one_failed",
    dag=dag
)

check_all_success = DummyOperator(
    task_id="t_check_all_success",
    trigger_rule="all_success",
    dag=dag
)


business_date_minus_one = '{{ (execution_date + macros.timedelta(days=-1)).strftime("%Y-%m-%d") }}'
DT_MACRO = '{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d") }}'

get_latest_exchange_rates = PythonOperator(
    task_id="t_get_latest_exchange_rates",
    python_callable=get_latest_exchange_rates,
    op_kwargs={
        "ingest_path": INGEST_PATH,
        "working_days_path": f"{INGEST_PATH}/working_days.csv",
        "currency_codes": config.CURRENCY_CODES,
        "url_nbp_api": config.URL_NBP_API,
        "business_date": DT_MACRO,
        "business_date_minus_one": business_date_minus_one
    },
    trigger_rule="one_success",
    dag=dag
)

# APPEND NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND TO THE DICTIONARY OF CURRENCY RATES
append_latest_exchange_rate = PythonOperator(
    task_id="t_append_latest_exchange_rate",
    python_callable=append_latest_exchange_rate,
    op_kwargs={
        "ingest_path": INGEST_PATH,
        "nbp_exchange_rates_latest": NBP_EXCHANGE_RATES_LATEST
    },
    trigger_rule="one_success",
    dag=dag
)

# MERGE NBP EXCHANGE RATES FOR PREVIOUS WORKING DAY IN POLAND AS DT (CURRENT EXECUTION DATE)
# TO DATES FROM EXCURSIONS TABLE
# A MERGE IS DONE BY REPLACEMENT OF PREVIOUS WORKING DAY WITH CURRENT DATE (DT)
merge_exchange_rates = PythonOperator(
    task_id="t_merge_exchange_rates",
    python_callable=merge_exchange_rates,
    op_kwargs={
        "curated_path": CURATED_PATH,
        "excursions": EXCURSIONS,
        "nbp_exchange_rates": NBP_EXCHANGE_RATES
    },
    trigger_rule="one_success",
    dag=dag
)

# CALCULATE VALUES FOR EXCURSIONS IN PLN
calculate_values = PythonOperator(
    task_id="t_calculate_values",
    python_callable=calculate_values,
    op_kwargs={
        "curated_path": CURATED_PATH,
        "business_ready_path": BUSINESS_READY_PATH
    },
    trigger_rule="one_success",
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

# SAVE TO AVRO
get_avro_output = PythonOperator(
    task_id="t_get_avro_output",
    python_callable=get_avro_output,
    op_kwargs={
        "business_ready_path": BUSINESS_READY_PATH
    },
    trigger_rule="one_success",
    dag=dag
)

#####################################################################################
# Set Relations Between Tasks
#####################################################################################
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
calculate_values >> get_avro_output
get_avro_output >> get_end_datetime
