from dateutil.relativedelta import *
import pandas as pd


ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"


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