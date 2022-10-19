import merge_tables
from datetime import datetime, timedelta

merge_tables.queues_rse_cric()
merge_tables.dataset_cric_replicas()
merge_tables.dataset_cric_replicas_v1('2022-02-03 04:00:00')

start_date = datetime(2021, 12, 10, 1, 0, 0)
end_date = datetime(2022, 2, 12, 1, 00, 0)
delta_day = timedelta(days=1)


while start_date <= end_date:
    print(start_date)
    merge_tables.dataset_cric_replicas(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    start_date += delta_day
    print('Data has been written!')