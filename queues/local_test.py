import queues_metrics
from datetime import datetime, timedelta


def collect_queues_for_period():

    start_date = datetime(2022, 10, 9, 20, 0, 0)
    end_date = datetime(2022, 10, 17, 8, 0, 0)
    # delta_day = timedelta(days=1)
    delta_hours = timedelta(hours=4)

    while start_date <= end_date:
        print(start_date)
        queues_metrics.queues_workload_weighted(predefined_date=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"),
                                 hours=4)
        start_date += delta_hours


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
            queues_metrics.queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=1)
            curr_date += delta_1hour

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_metrics.queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=3)
            curr_date += delta_3hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_metrics.queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=6)
            curr_date += delta_6hours

        curr_date = start_date
        while curr_date <= start_date + delta_day:
            print(curr_date)
            queues_metrics.queues_hourly_to_db(metric, predefined_date = curr_date, n_hours=12)
            curr_date += delta_12hours

        queues_metrics.queues_hourly_to_db(metric, predefined_date = start_date, n_hours=24)

        start_date += delta_day