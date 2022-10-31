SELECT datasetname,
       input_format_short,
       input_format_desc,
       regexp_replace(input_format_desc, '[[:digit:]]', '', 'g') as format_desc,
       input_project,
       prod_step,
       process_desc,
       n_dataset,
       tid,
       process_tags,
       sum(n_tasks) as n_tasks,
       min(datetime) as start_usage,
       max(datetime) as end_usage,
       DATE_PART('day', max(datetime) - min(datetime)) as usage_period
FROM datasets_popularity
WHERE datetime = date_trunc('day', TIMESTAMP :from_date)
--      datetime >= date_trunc('day', TIMESTAMP :from_date) - INTERVAL '1 week' and datetime < :from_date
GROUP BY datasetname,
         input_format_short,
         input_format_desc,
         regexp_replace(input_format_desc, '[[:digit:]]', '', 'g'),
         input_project,
         prod_step,
         process_desc,
         n_dataset,
         tid,
         process_tags