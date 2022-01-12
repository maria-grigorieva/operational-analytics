SELECT TRUNC(sysdate,'HH24') as datetime, datasetname,
       n_tasks, n_users
FROM
(
    SELECT
       d.datasetname,
       count(distinct t.jeditaskid) as n_tasks,
       count(distinct t.username) as n_users
    FROM ATLAS_PANDA.JEDI_TASKS t
    INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
    WHERE t.tasktype = 'anal'
    AND t.modificationtime >= sysdate - 7
    AND (d.datasetname LIKE 'mc%DAOD%' or d.datasetname LIKE 'data%DAOD%')
    AND d.type = 'input'
    AND t.endtime is not NULL
    AND d.masterid is null
    AND t.status in ('finished','done')
    GROUP BY d.datasetname)
    WHERE n_tasks > 10 and n_users > 1
ORDER BY n_tasks DESC
