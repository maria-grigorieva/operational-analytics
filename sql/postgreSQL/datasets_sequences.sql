SELECT datasetname,
       (array_agg(n_tasks::numeric ORDER BY date)) as n_tasks,
       CASE WHEN (array_agg(n_tasks::numeric ORDER BY date))[-1] > 0
            THEN 1 ELSE 0
        END as label
FROM
(SELECT
    t1.datasetname,
    date_trunc('day', cal)::date as date,
    COALESCE(t2.n_tasks,0) as n_tasks
FROM generate_series
    ( '2017-01-02'::timestamp
    , '2022-10-31'::timestamp
    , '1 week'::interval) cal
CROSS JOIN (SELECT DISTINCT datasetname FROM datasets_popularity
       WHERE input_format_short = 'DAOD'
    AND input_project = 'mc16_13TeV'
    AND input_format_desc LIKE 'HIGG%') t1
LEFT JOIN datasets_popularity t2
    ON t2.datetime = cal.date AND t2.datasetname = t1.datasetname
ORDER BY
    t1.datasetname,
    cal.date) as foo
GROUP BY foo.datasetname