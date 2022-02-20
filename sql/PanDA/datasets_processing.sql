with a as (
SELECT datasetname,
       pandaid,
       jeditaskid,
       queue,
       creationtime,
       starttime,
       endtime,
       finished_time,
       processingtype,
       cmtconfig,
       cpuconsumptionunit,
       username,
       cloud,
       nevents,
       assignedpriority,
       currentpriority,
       homepackage,
       atlasrelease,
       cpuconsumptiontime,
       actualcorecount,
       hs06sec,
       gshare,
       lag as queue_time,
       lead as running_time,
       round((finished_time - endtime)* 60 * 60 * 24) as postprocessing_time,
       dataset_n_files
FROM (
         SELECT ja.proddblock                                                                      as datasetname,
                ja.pandaid,
                ja.jeditaskid,
                ja.computingsite                                                                   as queue,
                ja.creationtime,
                ja.starttime,
                ja.endtime,
                ja.modificationtime as finished_time,
                ja.processingtype,
                ja.cmtconfig,
                ja.cpuconsumptionunit,
                ja.produsername as username,
                ja.cloud,
                ja.nevents,
                ja.assignedpriority,
                ja.currentpriority,
                ja.homepackage,
                ja.atlasrelease,
                ja.cpuconsumptiontime,
                ja.actualcorecount,
                ja.hs06sec,
                ja.gshare,
                js.jobstatus,
                js.modificationtime,
                d.status as dataset_status,
                d.nfiles as dataset_n_files,
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
         INNER JOIN ATLAS_PANDA.jedi_datasets d ON (ja.jeditaskid = d.jeditaskid and d.datasetname = ja.proddblock)
         WHERE ja.prodsourcelabel = 'user'
           AND ja.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
           AND ja.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours / 24
           AND (ja.proddblock LIKE 'mc%' or ja.proddblock LIKE 'data%')
           AND (ja.proddblock NOT LIKE '%debug%'
             OR ja.proddblock NOT LIKE '%scout%'
             OR ja.proddblock NOT LIKE '%hlt%'
             OR ja.proddblock NOT LIKE '%calibration%')
           AND ja.jobstatus = 'finished'
           AND js.jobstatus IN ('activated', 'running', 'transferring', 'merging', 'finished')
     )
WHERE jobstatus = 'running'),
     b as (
      SELECT pandaid, count(distinct fileid) as n_files_processed, sum(fsize) as bytes_processed
FROM ATLAS_PANDAARCH.filestable_arch
WHERE  pandaid in (select pandaid from ATLAS_PANDAARCH.JOBSARCHIVED
    WHERE modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
and modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours / 24
    AND prodsourcelabel='user'
    AND (proddblock LIKE 'mc%' or proddblock LIKE 'data%')
           AND (proddblock NOT LIKE '%debug%'
             OR proddblock NOT LIKE '%scout%'
             OR proddblock NOT LIKE '%hlt%'
             OR proddblock NOT LIKE '%calibration%')
           AND jobstatus = 'finished'
    ) and type = 'input'
group by pandaid
     )
SELECT a.*,b.n_files_processed,b.bytes_processed, round(b.n_files_processed*100/a.dataset_n_files,2) as percent_processed
FROM a,b WHERE a.pandaid = b.pandaid