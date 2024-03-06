from queues_metrics import queues_workload_extended, queues_hourly_to_db
from datetime import datetime, timedelta
import pandas as pd

def retrospective_collector(frequency='1h'):

    start_date = datetime(2023, 9, 28, 12, 0, 0)
    end_date = datetime(2023, 10, 10, 12, 0, 0) # next --> 2022-12-21 09:00:00
    #  {'from_date': '2022-09-02 03:00:00'} -- for queues_metrics_hourly

    list_of_dates = pd.date_range(start=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"),
                  end=datetime.strftime(end_date, "%Y-%m-%d %H:%M:%S"), freq=frequency)

    for i in list_of_dates[::-1]:
        print(i)
        queues_workload_extended(predefined_date=i, queues='db')
        # queues_hourly_to_db(metric='queues_hourly', predefined_date = i)
        # queues_workload_weighted_detailed(predefined_date = i)

queues_workload_extended(predefined_date='2023-09-28 13:00:00')
# retrospective_collector()
# queues_workload(predefined_date='2022-12-21 18:00:00', queues='actual')
#queues_metrics_hourly_new(predefined_date='2022-11-30 07:00:00', queues='actual')

def collect_queues_for_period():

    start_date = datetime(2022, 8, 21, 0, 0, 0)
    end_date = datetime(2022, 11, 22, 16, 0, 0)
    # delta_day = timedelta(days=1)
    delta_hours = timedelta(hours=4)

    while start_date <= end_date:
        print(start_date)
        queues_workload_extended(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"),
                                 hours=4, queues='db')
        start_date += delta_hours

# collect_queues_for_period()

def collect_hourly_data_for_period(metric):
    start_date = datetime(2022, 9, 15, 0, 0, 0)
    end_date = datetime(2022, 10, 1, 0, 0, 0)
    delta_day = timedelta(days=1)
    delta_1hour = timedelta(hours=1)
    delta_3hours = timedelta(hours=3)
    delta_6hours = timedelta(hours=6)
    delta_12hours = timedelta(hours=12)

    while start_date <= end_date:
        print(start_date)

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=1)
            curr_date += delta_1hour

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=3)
            curr_date += delta_3hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=6)
            curr_date += delta_6hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=12)
            curr_date += delta_12hours

        queues_hourly_to_db(metric, predefined_date = start_date, n_hours=24)

        start_date += delta_day