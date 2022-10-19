import tasks_analysis
from datetime import datetime, timedelta


start_date = datetime(2022, 1, 1, 1, 0, 0)
end_date = datetime(2022, 4, 20, 1, 00, 0)
delta_day = timedelta(days=1)

while start_date <= end_date:
    print(start_date)
    tasks_analysis.long_tasks_to_db(start_date)
    start_date += delta_day
