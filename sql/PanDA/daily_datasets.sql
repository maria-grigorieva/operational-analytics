SELECT datasetname,
       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') as datetime,
       queue,
       count(distinct jeditaskid) as n_tasks,
       count(distinct pandaid) as n_jobs,
       round(avg(lag)) as avg_queue_time,
       round(avg(lead)) as avg_running_time
FROM (
              SELECT d.datasetname,
           t.jeditaskid,
           js.pandaid,
       ja.computingsite as queue,
       js.jobstatus,
       js.modificationtime,
      ROUND((CAST(js.modificationtime as date) - (LAG(CAST(js.modificationtime as date), 1)
                                                 OVER (
                                                     PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC))) *
          60 * 60 * 24, 3)                                            as lag,
     ROUND((LEAD(CAST(js.modificationtime as date), 1)
                 OVER (
                     PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) -
            CAST(js.modificationtime as date)) * 60 * 60 * 24, 3) as       lead
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
        INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja ON (t.jeditaskid = ja.jeditaskid
                                                    and d.datasetname = ja.proddblock)
        INNER JOIN ATLAS_PANDA.JOBS_STATUSLOG js ON (js.pandaid = ja.pandaid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND t.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          AND t.status in ('finished','done','failed')
          AND js.jobstatus in ('activated','running','finished','failed')
          AND (d.datasetname LIKE 'mc%.DAOD%' or d.datasetname LIKE 'data%.DAOD%')
                  AND ( d.datasetname NOT LIKE '%debug%'
            OR d.datasetname NOT LIKE '%scout%'
            OR d.datasetname NOT LIKE '%hlt%'
            OR d.datasetname NOT LIKE '%calibration%')
          AND d.type = 'input'
          AND t.endtime is not NULL
          AND d.masterid is null
        ORDER BY d.datasetname,
           t.jeditaskid,
           js.pandaid,
       ja.computingsite)
WHERE jobstatus = 'running'
GROUP BY datasetname,trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD'),queue
