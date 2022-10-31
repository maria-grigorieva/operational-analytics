import timings
import pandas as pd
from datetime import datetime, timedelta

def collection_for_time_period():

    start_date = datetime(2022, 9, 7, 3, 0, 0)
    end_date = datetime(2022, 10, 27, 3, 0, 0)
    delta_day = timedelta(hours=24)

    while start_date <= end_date:
        print(start_date)
        timings.task_timings_to_db(predefined_date = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")) # 08.05.2022
        # task_timings_to_db(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"))
        # jobs_agg(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"))
        start_date += delta_day

collection_for_time_period()


def retrospective_dates():
    start_date = datetime(2022, 7, 10)
    end_date = datetime(2022, 10, 6)
    list_of_dates = pd.date_range(start=datetime.strftime(start_date, "%Y-%m-%d"),
                  end=datetime.strftime(end_date, "%Y-%m-%d"), freq='D')
    for i in list_of_dates[::-1]:
        timings.job_timings_to_db(predefined_date = datetime.strftime(i, "%Y-%m-%d"), hours=24)



# task_timings_to_db('2022-10-17 03:00:00')

timings.job_timings_to_db('2022-10-28 03:00:00')
# task_timings_to_db('2022-09-07 00:00:00', hours=1)
#
# jobs_agg('2022-06-11 00:00:00')