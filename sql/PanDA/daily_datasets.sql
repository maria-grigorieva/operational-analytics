SELECT datasetname,
       trunc(task_datetime,'DD') as datetime,
       queue,
       count(distinct jeditaskid) as n_tasks,
       count(distinct pandaid) as n_jobs,
       round(avg(lag)) as avg_queue_time,
       round(avg(lead)) as avg_running_time
FROM (
              SELECT d.datasetname,
           t.jeditaskid,
           t.modificationtime as task_datetime,
           js.pandaid,
       ja.computingsite as queue,
       js.jobstatus,
       js.modificationtime,
       LAG(CAST(js.modificationtime as date), 1)
                        OVER (
                            PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) as prev_state,
                    ROUND((CAST(js.modificationtime as date) - (LAG(CAST(js.modificationtime as date), 1)
                                                                 OVER (
                                                                     PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC))) *
                          60 * 60 * 24, 3)                                            as lag,
           LEAD(CAST(js.modificationtime as date), 1)
                          OVER (
                              PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) next_state_time,
                     ROUND((LEAD(CAST(js.modificationtime as date), 1)
                                 OVER (
                                     PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) -
                            CAST(js.modificationtime as date)) * 60 * 60 * 24, 3) as       lead
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
        INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED ja ON (t.jeditaskid = ja.jeditaskid
                                                    and d.datasetname = ja.proddblock)
        INNER JOIN ATLAS_PANDA.JOBS_STATUSLOG js ON (js.pandaid = ja.pandaid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date,'YYYY-MM-DD')
          AND t.modificationtime < to_date(:to_date, 'YYYY-MM-DD')
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
          --AND d.datasetname = 'data18_13TeV:data18_13TeV.00350160.physics_Main.deriv.DAOD_PHYS.r13100_p4795_p4856_tid27311157_00'
        ORDER BY d.datasetname,
           t.jeditaskid,
           js.pandaid,
       ja.computingsite,
       js.modificationtime)
WHERE jobstatus = 'running'
GROUP BY datasetname,trunc(task_datetime,'DD'),queue
UNION ALL
SELECT datasetname,
       trunc(task_datetime,'DD') as datetime,
       queue,
       count(distinct jeditaskid) as n_tasks,
       count(distinct pandaid) as n_jobs,
       round(avg(lag)) as avg_queue_time,
       round(avg(lead)) as avg_running_time
FROM (
              SELECT d.datasetname,
           t.jeditaskid,
           t.modificationtime as task_datetime,
           js.pandaid,
       ja.computingsite as queue,
       js.jobstatus,
       js.modificationtime,
       LAG(CAST(js.modificationtime as date), 1)
                        OVER (
                            PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) as prev_state,
                    ROUND((CAST(js.modificationtime as date) - (LAG(CAST(js.modificationtime as date), 1)
                                                                 OVER (
                                                                     PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC))) *
                          60 * 60 * 24, 3)                                            as lag,
           LEAD(CAST(js.modificationtime as date), 1)
                          OVER (
                              PARTITION BY d.datasetname,t.jeditaskid,t.modificationtime,js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) next_state_time,
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
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD')
          AND t.modificationtime < to_date(:to_date, 'YYYY-MM-DD')
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
          --AND d.datasetname = 'data18_13TeV:data18_13TeV.00350160.physics_Main.deriv.DAOD_PHYS.r13100_p4795_p4856_tid27311157_00'
        ORDER BY d.datasetname,
           t.jeditaskid,
           js.pandaid,
       ja.computingsite,
       js.modificationtime)
WHERE jobstatus = 'running'
GROUP BY datasetname,trunc(task_datetime,'DD'),queue
