import datetime
import pandas as pd
import calendar


def create_table(day: datetime.datetime = None):
    if day is None:
        day = datetime.datetime.today()

    days = []
    for i in range(-4, 5):
        c = day + datetime.timedelta(days=i)
        s = f'{calendar.day_name[c.weekday()]} {c.day}'
        days.append(s)

    df = pd.DataFrame("", columns=days, index=[i for i in range(24)])

    return df
