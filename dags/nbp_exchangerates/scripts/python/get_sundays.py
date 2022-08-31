from dateutil.relativedelta import *
import dateutil.rrule as rrule
from datetime import datetime
import pandas as pd

def get_sundays(years):
    """
    :param years:
    :return:
    """

    before=datetime(min(years), 1, 1)
    after=datetime(max(years), 12, 31)
    rr = rrule.rrule(rrule.WEEKLY, byweekday=SU, dtstart=before)

    output = pd.DataFrame(rr.between(before, after, inc=True), columns=['date'])
    output['holiday'] = 'Sunday'

    print(output.head())

    return output
