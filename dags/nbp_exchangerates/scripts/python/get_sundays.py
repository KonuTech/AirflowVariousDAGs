from dateutil.relativedelta import *
import dateutil.rrule as rrule
from datetime import datetime
import pandas as pd


ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"


def get_sundays(years, ingest_path=INGEST_PATH):
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

    # Save output
    # print(output.tail())
    # with open(f'{ingest_path}/sundays.txt', 'w') as f:
    #     f.write(output.to_string())

    output.to_csv(f'{ingest_path}/sundays.csv', index=False)
