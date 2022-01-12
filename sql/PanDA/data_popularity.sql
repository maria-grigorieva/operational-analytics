with tasks as (
    SELECT t.jeditaskid,t.status as task_status,t.gshare,t.username,
        d.datasetname,d.containername,d.datasetid,
        t.starttime as task_start_time,
        t.creationdate as task_creationdate,
        t.endtime      as task_end_time,
        t.modificationtime as task_modificationtime,
        d.status as dataset_status,
        d.nfilesfinished,d.nfilesfailed,d.nfiles,d.nfilestobeused,
        ja.pandaid,ja.creationtime,ja.starttime,ja.endtime,ja.jobstatus,ja.computingsite,
        ja.ninputdatafiles,ja.inputfilebytes,ja.outputfilebytes,ja.noutputdatafiles,ja.cpuconsumptiontime,
        ja.cloud,
        sum(CASE WHEN d.datasetid = f.datasetid THEN f.fsize ELSE 0 END) as primary_input_fsize,
        sum(CASE WHEN d.datasetid != f.datasetid
                   AND f.proddblock is not Null THEN f.fsize ELSE 0 END) as secondary_input_fsize
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
        INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED ja ON (t.jeditaskid = ja.jeditaskid
                                                    and d.datasetname = ja.proddblock)
         RIGHT OUTER JOIN ATLAS_PANDAARCH.filestable_arch f ON (t.jeditaskid = f.jeditaskid and
                                                                ja.pandaid = f.pandaid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
          AND t.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours/24
          AND t.creationdate >= sysdate - 100
          AND t.status in ('finished','failed','done','broken','aborted')
          AND (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
                  AND ( d.datasetname NOT LIKE '%debug%'
            OR d.datasetname NOT LIKE '%scout%'
            OR d.datasetname NOT LIKE '%hlt%'
            OR d.datasetname NOT LIKE '%calibration%')
          AND d.type = 'input'
          AND t.endtime is not NULL
          AND d.masterid is null
          AND f.type = 'input'
            GROUP BY t.jeditaskid,t.status,t.gshare,t.username,
                     d.datasetname,d.containername,d.datasetid,
                     t.starttime,t.creationdate,t.endtime,t.modificationtime,
                     d.status,
                    d.nfilesfinished,d.nfilesfailed,d.nfiles,d.nfilestobeused,
                    ja.pandaid,ja.creationtime,ja.starttime,ja.endtime,ja.jobstatus,ja.computingsite,
                    ja.ninputdatafiles,ja.inputfilebytes,ja.outputfilebytes,ja.noutputdatafiles,
                    ja.cpuconsumptiontime,
                    ja.cloud
    UNION
    SELECT t.jeditaskid,t.status as task_status,t.gshare,t.username,
        d.datasetname,d.containername,d.datasetid,
        t.starttime as task_start_time,
        t.creationdate as task_creationdate,
        t.endtime      as task_end_time,
        t.modificationtime as task_modificationtime,
        d.status as dataset_status,
        d.nfilesfinished,d.nfilesfailed,d.nfiles,d.nfilestobeused,
        ja.pandaid,ja.creationtime,ja.starttime,ja.endtime,ja.jobstatus,ja.computingsite,
        ja.ninputdatafiles,ja.inputfilebytes,ja.outputfilebytes,ja.noutputdatafiles,
        ja.cpuconsumptiontime,
        ja.cloud,
                   sum(CASE WHEN d.datasetid = f.datasetid THEN f.fsize ELSE 0 END) as primary_input_fsize,
        sum(CASE WHEN d.datasetid != f.datasetid
                   AND f.proddblock is not Null THEN f.fsize ELSE 0 END) as secondary_input_fsize
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
        INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja ON (t.jeditaskid = ja.jeditaskid
                                                    and d.datasetname = ja.proddblock)
        RIGHT OUTER JOIN ATLAS_PANDA.filestable4 f ON (t.jeditaskid = f.jeditaskid and
                                                 ja.pandaid = f.pandaid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
          AND t.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours/24
          AND t.creationdate >= sysdate - 100
          AND t.status in ('finished','failed','done','broken','aborted')
          AND (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
                  AND ( d.datasetname NOT LIKE '%debug%'
            OR d.datasetname NOT LIKE '%scout%'
            OR d.datasetname NOT LIKE '%hlt%'
            OR d.datasetname NOT LIKE '%calibration%')
          AND d.type = 'input'
          AND t.endtime is not NULL
          AND d.masterid is null
          AND f.type = 'input'
            GROUP BY t.jeditaskid,t.status,t.gshare,t.username,
                     d.datasetname,d.containername,d.datasetid,
                     t.starttime,t.creationdate,t.endtime,t.modificationtime,
                     d.status,
                     d.nfilesfinished,d.nfilesfailed,d.nfiles,d.nfilestobeused,
                    ja.pandaid,ja.creationtime,ja.starttime,ja.endtime,ja.jobstatus,ja.computingsite,
                    ja.ninputdatafiles,ja.inputfilebytes,ja.outputfilebytes,ja.noutputdatafiles,
                    ja.cpuconsumptiontime,
                    ja.cloud
),
  attempts_pivot as (
    SELECT jeditaskid,
           attemptnr as task_attemptnr,
           min(rerefine_first) as attempt_rerefined_first,
           min(defined_first) as attempt_defined_first,
           min(ready_first) as attempt_ready_first,
           min(running_first) as attempt_running_first,
           min(scouting_first) as attempt_scouting_first,
           NVL(min(scouting_first),min(running_first)) as attempt_start,
          GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
              NVL(max(broken_last),(SYSDATE - 365)),
              NVL(max(failed_last),(SYSDATE - 365)),
              NVL(max(done_last),(SYSDATE - 365)),
              NVL(max(aborted_last),(SYSDATE - 365))) as attempt_finished,
          CASE
              WHEN NVL(max(finished_last),(SYSDATE - 365)) >
                       GREATEST(NVL(max(broken_last),(SYSDATE - 365)),
                              NVL(max(failed_last),(SYSDATE - 365)),
                              NVL(max(done_last),(SYSDATE - 365)),
                              NVL(max(aborted_last),(SYSDATE - 365)))
                   THEN 'finished'
                WHEN NVL(max(broken_last),(SYSDATE - 365)) >
                     GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                              NVL(max(failed_last),(SYSDATE - 365)),
                              NVL(max(done_last),(SYSDATE - 365)),
                              NVL(max(aborted_last),(SYSDATE - 365)))
                     THEN 'broken'
                WHEN NVL(max(failed_last),(SYSDATE - 365)) >
                        GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                              NVL(max(broken_last),(SYSDATE - 365)),
                              NVL(max(done_last),(SYSDATE - 365)),
                              NVL(max(aborted_last),(SYSDATE - 365)))
                      THEN 'failed'
                WHEN NVL(max(done_last),(SYSDATE - 365)) >
                          GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                              NVL(max(broken_last),(SYSDATE - 365)),
                              NVL(max(failed_last),(SYSDATE - 365)),
                              NVL(max(aborted_last),(SYSDATE - 365)))
                    THEN 'done'
                WHEN NVL(max(aborted_last),(SYSDATE - 365)) >
                          GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                              NVL(max(broken_last),(SYSDATE - 365)),
                              NVL(max(failed_last),(SYSDATE - 365)),
                              NVL(max(done_last),(SYSDATE - 365)))
              THEN 'aborted'
              ELSE 'not completed'
          END as attempt_status,
          round(sum(defined_delta), 3) as attempt_defined_delta,
          round(sum(ready_delta), 3) as attempt_ready_delta,
          round(sum(running_delta), 3) as attempt_running_delta,
          round(sum(pending_delta), 3) as attempt_pending_delta,
          round(sum(throttled_delta), 3) as attempt_throttled_delta,
          round(sum(exhausted_delta), 3) as attempt_exhausted_delta,
          ROUND((CAST(min(running_first) as date) - CAST(min(ready_first) as date))*24*60*60, 2) as attempt_ready_run_delay
    FROM (
        SELECT jeditaskid,
                 attemptnr,
                 status,
                 modificationtime as ttimestamp,
                 ROUND((LEAD(CAST(modificationtime as date), 1)
                             OVER (
                                 PARTITION BY jeditaskid, attemptnr ORDER BY modificationtime ASC) -
                        CAST(modificationtime as date)) * 60 * 60 * 24, 3)                  delta
          FROM ATLAS_PANDA.TASKS_STATUSLOG
          WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
        )
    PIVOT (
        min(ttimestamp) as first,
        max(ttimestamp) as last,
        sum(delta) as delta
        FOR status
        IN ('defined' AS defined,
          'ready' AS ready,
          'running' AS running,
          'pending' AS pending,
          'scouting' AS scouting,
          'throttled' AS throttled,
          'exhausted' AS exhausted,
          'finished' AS finished,
          'done' AS done,
          'failed' AS failed,
          'aborted' AS aborted,
          'broken' AS broken,
          'rerefine' as rerefine
          )
    )
    GROUP BY jeditaskid, attemptnr
      )
    SELECT
           tasks.jeditaskid,
           tasks.task_status,
           tasks.gshare,
           tasks.username,
           tasks.task_creationdate,
           tasks.task_start_time,
           tasks.task_end_time,
           tasks.task_modificationtime,
           tasks.datasetname,
           tasks.containername,
           tasks.dataset_status,
           attempts_pivot.task_attemptnr,
           attempts_pivot.attempt_status,
           tasks.computingsite as queue,
           tasks.jobstatus as jobstatus,
           NVL(count(tasks.pandaid),0) as number_of_jobs,
           min(tasks.creationtime) as jobs_creationtime,
           min(tasks.starttime) as jobs_starttime,
           max(tasks.endtime) as jobs_endtime,
           avg(tasks.nfilesfinished) as ds_nfilesfininshed,
           avg(tasks.nfilesfailed) as ds_nfilesfailed,
           avg(tasks.nfiles) as ds_nfiles,
           min(attempts_pivot.attempt_rerefined_first) as attempt_rerefined_first,
           round((max(tasks.endtime) - min(tasks.starttime))*24*60*60,2) as jobs_total_duration,
           sum(round((tasks.endtime - tasks.starttime)*24*60*60,2)) as jobs_total_walltime,
           round((min(tasks.starttime) - min(tasks.creationtime))*24*60*60,2) as jobs_timetostart,
           sum(tasks.ninputdatafiles) as jobs_total_ninputdatafiles,
           sum(tasks.inputfilebytes) as jobs_total_inputfilebytes,
           sum(tasks.outputfilebytes) as jobs_total_outputfilebytes,
           sum(tasks.noutputdatafiles) as jobs_total_noutputdatafiles,
           sum(tasks.primary_input_fsize) as jobs_primary_input_fsize,
           sum(tasks.secondary_input_fsize) as jobs_secondary_input_fsize,
           sum(tasks.cpuconsumptiontime) as jobs_cpuconsumptiontime,
           round(((CAST(max(attempts_pivot.attempt_finished) as date) -
           CAST(min(attempts_pivot.attempt_start) as date))),2)*24*60*60 as attempt_total_duration,
           avg(attempts_pivot.attempt_defined_delta) as attempt_defined_delta,
           TO_CHAR(min(attempts_pivot.attempt_defined_first),'YYYY-MM-DD HH24:MI:SS') as attempt_defined_first,
           TO_CHAR(min(attempts_pivot.attempt_scouting_first),'YYYY-MM-DD HH24:MI:SS') as attempt_scouting_first,
           avg(attempts_pivot.attempt_exhausted_delta) as attempt_exhausted_delta,
           avg(attempts_pivot.attempt_pending_delta) as attempt_pending_delta,
           avg(attempts_pivot.attempt_ready_delta) as attempt_ready_delta,
           TO_CHAR(min(attempts_pivot.attempt_ready_first),'YYYY-MM-DD HH24:MI:SS') as attempt_ready_first,
           avg(attempts_pivot.attempt_running_delta) as attempt_running_delta,
           TO_CHAR(min(attempts_pivot.attempt_running_first),'YYYY-MM-DD HH24:MI:SS') as attempt_running_first,
           avg(attempts_pivot.attempt_ready_run_delay) as attempt_ready_run_delay,
           TO_CHAR(min(attempts_pivot.attempt_start),'YYYY-MM-DD HH24:MI:SS') as attempt_start,
           avg(attempts_pivot.attempt_throttled_delta) as attempt_throttled_delta,
           TO_CHAR(max(attempts_pivot.attempt_finished),'YYYY-MM-DD HH24:MI:SS') as attempt_finished
        FROM attempts_pivot
        INNER JOIN tasks ON (tasks.jeditaskid = attempts_pivot.jeditaskid)
        WHERE (tasks.starttime >= attempts_pivot.attempt_start or tasks.creationtime >= attempts_pivot.attempt_ready_first)
          AND tasks.endtime <= attempts_pivot.attempt_finished
        GROUP BY tasks.jeditaskid,
                 tasks.task_status,
                 tasks.gshare,
                 tasks.username,
                 tasks.task_creationdate,
                 tasks.task_start_time,
                 tasks.task_end_time,
                 tasks.task_modificationtime,
                 tasks.datasetname,
                 tasks.containername,
                 tasks.dataset_status,
                 attempts_pivot.task_attemptnr,
                 attempts_pivot.attempt_status,
                 tasks.computingsite,
                 tasks.jobstatus