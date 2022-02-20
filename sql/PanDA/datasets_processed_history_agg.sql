with a as (
SELECT datasetname,
       pandaid,
       jeditaskid,
       queue,
       creationtime,
       starttime,
       endtime,
       finished_time,
       cloud,
       project,
       data_type,
       cpuconsumptiontime,
       lag as queue_time,
       lead as running_time,
       round((finished_time - endtime)* 60 * 60 * 24) as postprocessing_time
FROM (
         SELECT ja.proddblock                                                                      as datasetname,
                ja.pandaid,
                ja.jeditaskid,
                ja.computingsite                                                                   as queue,
                ja.creationtime,
                ja.starttime,
                ja.endtime,
                ja.modificationtime as finished_time,
                ja.cloud,
                ja.cpuconsumptiontime,
                ja.inputfileproject as project,
                TRIM('-' FROM REGEXP_REPLACE(ja.inputfiletype,'\d','-')) as data_type,
                js.jobstatus,
                js.modificationtime,
                LAG(CAST(js.modificationtime as date), 1)
                    OVER (
                        PARTITION BY js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) as prev_state,
                ROUND((CAST(js.modificationtime as date) - (LAG(CAST(js.modificationtime as date), 1)
                                                                OVER (
                                                                    PARTITION BY js.pandaid,js.computingsite ORDER BY js.modificationtime ASC))) *
                      60 * 60 * 24, 3)                                                             as lag,
                LEAD(CAST(js.modificationtime as date), 1)
                     OVER (
                         PARTITION BY js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC)   next_state_time,
                ROUND((LEAD(CAST(js.modificationtime as date), 1)
                            OVER (
                                PARTITION BY js.pandaid,ja.computingsite ORDER BY js.modificationtime ASC) -
                       CAST(js.modificationtime as date)) * 60 * 60 * 24, 3)                       as lead
         FROM ATLAS_PANDAARCH.JOBSARCHIVED ja
                 INNER JOIN ATLAS_PANDA.JOBS_STATUSLOG js ON (ja.pandaid = js.pandaid)
         WHERE ja.prodsourcelabel = 'user'
           AND ja.modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
           AND ja.modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
           AND (ja.proddblock LIKE 'mc%' or ja.proddblock LIKE 'data%')
           AND (ja.proddblock NOT LIKE '%debug%'
             OR ja.proddblock NOT LIKE '%scout%'
             OR ja.proddblock NOT LIKE '%hlt%'
             OR ja.proddblock NOT LIKE '%calibration%')
           AND ja.jobstatus = 'finished'
           AND js.jobstatus IN ('activated', 'running', 'transferring', 'merging', 'finished')
        AND ja.gshare = 'User Analysis'
     )
WHERE jobstatus = 'running'),
     b as (
      SELECT pandaid, count(distinct fileid) as n_files_processed, sum(fsize) as bytes_processed
FROM ATLAS_PANDAARCH.filestable_arch
WHERE  pandaid in (select pandaid from ATLAS_PANDAARCH.JOBSARCHIVED
    WHERE modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
and modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
    AND prodsourcelabel='user'
    AND (proddblock LIKE 'mc%' or proddblock LIKE 'data%')
           AND (proddblock NOT LIKE '%debug%'
             OR proddblock NOT LIKE '%scout%'
             OR proddblock NOT LIKE '%hlt%'
             OR proddblock NOT LIKE '%calibration%')
           AND jobstatus = 'finished' AND gshare = 'User Analysis'
    ) and type = 'input'
group by pandaid
     )
SELECT a.queue,
       a.cloud,
       a.project,
       a.data_type,
       trunc(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD') as datetime,
       count(distinct a.datasetname) as n_datasets,
       count(distinct a.pandaid) as n_jobs,
       round(sum(b.n_files_processed),0) as files_processed,
       round(sum(b.bytes_processed),0) as bytes_processed,
       round(avg(a.queue_time),0) as avg_queue_time,
       round(avg(b.bytes_processed/nullif(a.running_time,0)), 0) as avg_running_speed,
       round(avg(a.postprocessing_time),0) as avg_postprocessing_time
FROM a,b WHERE a.pandaid = b.pandaid
GROUP BY a.queue,
       a.cloud,
                a.project,
       a.data_type,
         trunc(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')