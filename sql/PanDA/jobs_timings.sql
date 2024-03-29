with jobs as (
    SELECT j.jeditaskid,
           ta.attemptnr as task_attemptnr,
           ta.endstatus as task_attempt_endstatus,
                     s.pandaid,
                     s.jobstatus,
                     s.computingsite as queue,
                     j.proddblock as datasetname,
                     s.modificationtime,
                     j.modificationtime as completed_tstamp,
                     j.ninputdatafiles,
                     j.noutputdatafiles,
                     j.inputfilebytes,
                     j.outputfilebytes,
                     j.inputfiletype,
                     j.inputfileproject,
                     j.corecount,
                     j.actualcorecount,
                     j.nevents,
                     j.computingelement,
                     j.cpuconsumptionunit,
                     j.cpuconsumptiontime,
                     j.jobstatus as status,
                     j.assignedpriority,
                     j.currentpriority,
                     j.atlasrelease,
                     j.transformation,
                     j.homepackage,
                     j.starttime,
                     j.endtime,
                     j.creationtime,
                     j.hs06sec,
                     j.processingtype,
                     f.fsize as lib_size,
                    CASE WHEN j.specialhandling LIKE '%sj%' THEN 'scout'
                         WHEN j.specialhandling LIKE '%debug%' THEN 'debug'
                         WHEN j.specialhandling LIKE '%express%' THEN 'express'
                     ELSE 'other'
                     END as job_type,
                     j.avgrss,
                     j.avgpss,
                     j.avgswap,
                     j.avgvmem,
                     j.raterbytes,
                     j.ratewbytes,
                     j.totrbytes,
                     j.totwbytes
              FROM ATLAS_PANDA.JOBS_STATUSLOG s
                  INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON (j.pandaid = s.pandaid)
                       INNER JOIN ATLAS_PANDAARCH.FILESTABLE_ARCH f ON (j.jeditaskid = f.jeditaskid AND f.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.TASK_ATTEMPTS ta ON (ta.jeditaskid = j.jeditaskid AND j.creationtime >= ta.starttime and j.modificationtime <= ta.endtime)
              WHERE
                  j.modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
              AND j.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') + :hours / 24
              AND ta.endstatus in ('finished','failed','done','broken','aborted')
              AND j.prodsourcelabel = 'user'
              AND j.processingtype not like 'gangarobot%'
              AND j.produsername not in ('artprod','gangarbt')
              AND j.gshare = 'User Analysis'
              AND (j.proddblock LIKE 'mc%' OR j.proddblock LIKE 'data%')
                AND ( j.proddblock NOT LIKE '%debug%'
                OR j.proddblock NOT LIKE '%scout%'
                OR j.proddblock NOT LIKE '%hlt%'
                OR j.proddblock NOT LIKE '%calibration%')
              AND f.dataset LIKE '%.lib.%'
              AND j.jobstatus = 'finished'
              UNION
              (
                SELECT j.jeditaskid,
           ta.attemptnr as task_attemptnr,
           ta.endstatus as task_attempt_endstatus,
                     s.pandaid,
                     s.jobstatus,
                     s.computingsite as queue,
                     j.proddblock as datasetname,
                     s.modificationtime,
                     j.modificationtime as completed_tstamp,
                     j.ninputdatafiles,
                     j.noutputdatafiles,
                     j.inputfilebytes,
                     j.outputfilebytes,
                     j.inputfiletype,
                     j.inputfileproject,
                     j.corecount,
                     j.actualcorecount,
                     j.nevents,
                     j.computingelement,
                     j.cpuconsumptionunit,
                     j.cpuconsumptiontime,
                     j.jobstatus as status,
                     j.assignedpriority,
                     j.currentpriority,
                     j.atlasrelease,
                     j.transformation,
                     j.homepackage,
                     j.starttime,
                     j.endtime,
                     j.creationtime,
                     j.hs06sec,
                     j.processingtype,
                     f.fsize as lib_size,
                    CASE WHEN j.specialhandling LIKE '%sj%' THEN 'scout'
                         WHEN j.specialhandling LIKE '%debug%' THEN 'debug'
                         WHEN j.specialhandling LIKE '%express%' THEN 'express'
                     ELSE 'other'
                     END as job_type,
                     j.avgrss,
                     j.avgpss,
                     j.avgswap,
                     j.avgvmem,
                     j.raterbytes,
                     j.ratewbytes,
                     j.totrbytes,
                     j.totwbytes
                  FROM ATLAS_PANDA.JOBS_STATUSLOG s
                  INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 j ON (s.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.FILESTABLE4 f ON (j.jeditaskid = f.jeditaskid AND f.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.TASK_ATTEMPTS ta ON (ta.jeditaskid = j.jeditaskid AND j.creationtime >= ta.starttime and j.modificationtime <= ta.endtime)
              WHERE
                  j.modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
              AND j.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') + :hours / 24
              AND ta.endstatus in ('finished','failed','done','broken','aborted')
              AND j.prodsourcelabel = 'user'
              AND j.processingtype not like 'gangarobot%'
              AND j.produsername not in ('artprod','gangarbt')
              AND j.gshare = 'User Analysis'
              AND (j.proddblock LIKE 'mc%' OR j.proddblock LIKE 'data%')
                AND ( j.proddblock NOT LIKE '%debug%'
                OR j.proddblock NOT LIKE '%scout%'
                OR j.proddblock NOT LIKE '%hlt%'
                OR j.proddblock NOT LIKE '%calibration%')
              AND f.dataset LIKE '%.lib.%'
              AND j.jobstatus = 'finished'
    )
),
    jobs_attempts as (SELECT jobs.*,
                             LEAD(CAST(modificationtime as date), 1)
                                  OVER (
                                      PARTITION BY jeditaskid, task_attemptnr, pandaid ORDER BY task_attemptnr,modificationtime ASC) as lead_timestamp,
                             ROUND((LEAD(CAST(modificationtime as date), 1)
                                         OVER (
                                             PARTITION BY jeditaskid, task_attemptnr, pandaid ORDER BY task_attemptnr,modificationtime ASC) -
                                    CAST(modificationtime as date)) * 60 * 60 * 24, 3)                                   lead
                      FROM jobs
                      ),
    result as (SELECT jeditaskid,
                      task_attemptnr,
                      pandaid,
                      queue,
                      datasetname,
                      ninputdatafiles,
                      noutputdatafiles,
                      inputfilebytes,
                      outputfilebytes,
                      lib_size,
                    MIN(regexp_replace(SUBSTR(datasetname, REGEXP_INSTR(datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(datasetname, '\.', 1, 4)-1)),'[0-9]','')) as input_format,
                MIN(regexp_substr(regexp_replace(SUBSTR(datasetname, REGEXP_INSTR(datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1)) as input_format_short,
                MIN(regexp_substr(regexp_replace(SUBSTR(datasetname, REGEXP_INSTR(datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2)) as input_format_desc,
                    MIN(SUBSTR(datasetname, 1, Instr(datasetname, ':', -1, 1)-1)) as input_project,
                      corecount,
                      actualcorecount,
                      nevents,
                      computingelement,
                      cpuconsumptionunit,
                      cpuconsumptiontime,
                      status as job_status,
                      assignedpriority,
                      currentpriority,
                      atlasrelease,
                      transformation,
                      homepackage,
                      processingtype,
                      hs06sec,
                      starttime as execution_start_tstamp,
                      endtime as execution_end_tstamp,
                      completed_tstamp,
                      creationtime                                       as creation_tstamp,
                      job_type,
                      avgrss,
                      avgpss,
                      avgswap,
                      avgvmem,
                      raterbytes,
                      ratewbytes,
                      totrbytes,
                      totwbytes,
                      min(transferring_first)                            as transferring_tstamp,
                      min(merging_first)                                 as merging_tstamp,
                      ROUND((CAST(starttime as date) -
                             CAST(creationtime as date)) * 24 * 60 * 60) as waiting_time,
                      ROUND((CAST(endtime as date) -
                             CAST(starttime as date)) * 24 * 60 * 60) as execution_time,
                      ROUND((CAST(completed_tstamp as date) -
                             CAST(creationtime as date)) * 24 * 60 * 60) as total_time,
                      sum(transferring_lead)                             as transferring_time,
                      sum(merging_lead)                                  as merging_time
               FROM jobs_attempts
                   PIVOT (
                   min(modificationtime) as first,
                       max(modificationtime) as last,
                       sum(lead) as lead
                   FOR jobstatus
                   IN ('defined' AS defined,
                       'activated' AS activated,
                       'running' AS running,
                       'pending' AS pending,
                       'sent' AS sent,
                       'starting' AS starting,
                       'transferring' AS transferring,
                       'finished' AS finished,
                       'failed' AS failed,
                       'holding' AS holding,
                       'aborted' AS aborted,
                       'broken' AS broken,
                       'merging' as merging,
                       'cancelled' as cancelled,
                       'closed' as closed,
                       'assigned' as assigned
                       )
                   )
               GROUP BY jeditaskid,
                      task_attemptnr,
                      pandaid,
                      queue,
                      datasetname,
                      ninputdatafiles,
                      noutputdatafiles,
                      inputfilebytes,
                      outputfilebytes,
                      lib_size,
                      corecount,
                      actualcorecount,
                      nevents,
                      computingelement,
                      cpuconsumptionunit,
                      cpuconsumptiontime,
                      status,
                      assignedpriority,
                      currentpriority,
                      atlasrelease,
                      transformation,
                      homepackage,
                      hs06sec,
                      processingtype,
                        starttime,
                        endtime,
                        completed_tstamp,
                        creationtime,
                      job_type,
                      avgrss,
                      avgpss,
                      avgswap,
                      avgvmem,
                      raterbytes,
                      ratewbytes,
                      totrbytes,
                      totwbytes)
SELECT result.*
FROM result