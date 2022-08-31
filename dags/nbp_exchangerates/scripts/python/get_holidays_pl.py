from datetime import date, timedelta
from dateutil.relativedelta import *
from dateutil import easter
import pandas as pd

ROOT_PATH_DAG = "/usr/local/airflow/dags/nbp_exchangerates"
INGEST_PATH = f"{ROOT_PATH_DAG}/ingest"

def get_holidays_pl(years, ingest_path):
    """
    :param years:
    :param ingest_path:
    :return: None
    """

    df = pd.DataFrame()

    for year in years:

        # Get holidays
        easter_sunday = easter.easter(year)
        holidays = {'New Year': date(year,1,1),
                    'Trzech Kroli': date(year,1,6),
                    'Easter Sunday': easter_sunday,
                    'Easter Monday': easter_sunday + timedelta(days=1),
                    'Labor Day': date(year,5,1),
                    'Constitution Day': date(year,5,3),
                    'Pentecost Sunday': easter_sunday + relativedelta(days=+1, weekday=SU(+7)),
                    'Corpus Christi': easter_sunday + relativedelta(weekday=TH(+9)),
                    'Assumption of the Blessed Virgin Mary': date(year,8,15),
                    'All Saints\' Day': date(year,11,1),
                    'Independence Day': date(year,11,11),
                    'Christmas  Day': date(year, 12, 25),
                    'Boxing Day': date(year, 12, 26),
                    }

        # Get output
        df = pd.concat([df, pd.DataFrame([holidays])], ignore_index=True)
        df.index.names = ['id']
        output = df.transpose(copy=True)
        output = output.stack().reset_index(name='date').rename(columns={'level_0':'holiday', 'id': 'year_id'})
        output = output.drop(columns='year_id')
        output['date'] = pd.to_datetime(output['date'])


    # Save output
    print(output.tail())
    with open(f'{ingest_path}/holidays_pl.txt', 'w') as f:
        f.write(output.to_string())
