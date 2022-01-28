from google.cloud import bigquery
import pandas_gbq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/data_placement/conf/atlas-336515-9bbd95e3dadf.json"
#
# client = bigquery.Client()


# query_job = client.query(
#     """
#     SELECT * FROM `atlas-336515.analytix.datasets_snapshot` WHERE DATE(_PARTITIONTIME) = "2022-01-21" LIMIT 1000"""
# )
#
# results = query_job.result()
#
# for row in results:
#         print(row)

sql = """
SELECT * FROM `atlas-336515.analytix.datasets_snapshot` WHERE DATE(_PARTITIONTIME) = "2022-01-21" LIMIT 1000
"""
df = pandas_gbq.read_gbq(sql)
print(df)

pandas_gbq.to_gbq(df, 'digital_cases.test', project_id='atlas-336515', if_exists='append')


