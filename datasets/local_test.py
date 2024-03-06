from datasets_popularity import datasets_at_queues
from datetime import datetime, timedelta
import pandas as pd

def retrospective_collector(frequency='24h'):

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 31)

    list_of_dates = pd.date_range(start=datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S"),
                  end=datetime.strftime(end_date, "%Y-%m-%d %H:%M:%S"), freq=frequency)

    for i in list_of_dates[::-1]:
        print(i)
        datasets_at_queues(predefined_date=i)

retrospective_collector()