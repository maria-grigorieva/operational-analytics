import resource_weights
from datetime import datetime, timedelta

start_date = datetime(2022, 9, 15, 3, 0, 0)
end_date = datetime(2022, 10, 1, 0, 0, 0)
delta = timedelta(hours=3)

while start_date <= end_date:
    print(start_date)
    resource_weights.calculate_queue_weights_weighted(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    # calculate_queue_weights_hourly_enhanced(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    # calculate_queue_weights(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    # calculate_weights_overall(datetime.strftime(start_date,"%Y-%m-%d %H:%M:%S"))
    start_date += delta
    print('Data has been written!')
