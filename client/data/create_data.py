import calendar
import datetime

import pandas as pd

from proto.events_pb2 import Event


def create_table(user_events: dict[int, Event], day: datetime.date = None):
    if day is None:
        day = datetime.datetime.today()

    days = []
    data = [[] for _ in range(24)]

    for i in range(-4, 5):
        c = day + datetime.timedelta(days=i)
        s = f'{calendar.day_name[c.weekday()]} {c.day}'
        days.append(s)
        for j in range(24):
            for i in user_events:
                start = user_events[i].start.ToDatetime()
                end = user_events[i].end.ToDatetime()
                current = datetime.datetime.combine(c, datetime.time(j))
                if start <= current <= end:
                    data[j].append(" ")
                    break
            else:
                data[j].append("")

    df = pd.DataFrame(data, columns=days, index=[i for i in range(24)])

    def color_survived(val: str):
        color = 'lightgreen' if val != '' else ''
        return f'background-color: {color}'

    df = df.style.applymap(color_survived, subset=days)



    return df
