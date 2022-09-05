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
                     j.cpuconsumptionunit,
                     j.cpuconsumptiontime,
                     j.jobstatus as status,
                     j.assignedpriority,
                     j.currentpriority,
                     j.atlasrelease,
                     j.transformation,
                     j.homepackage,
                     j.resource_type,
                     j.starttime,
                     j.endtime,
                     j.creationtime,
                     j.hs06sec,
                     j.processingtype,
                     f.fsize as lib_size
              FROM ATLAS_PANDA.JOBS_STATUSLOG s
                  INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON (j.pandaid = s.pandaid)
                       INNER JOIN ATLAS_PANDAARCH.FILESTABLE_ARCH f ON (j.jeditaskid = f.jeditaskid AND f.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.TASK_ATTEMPTS ta ON (ta.jeditaskid = j.jeditaskid AND j.creationtime >= ta.starttime and j.modificationtime <= ta.endtime)
              WHERE
                  j.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
              AND j.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours / 24
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
                     j.cpuconsumptionunit,
                     j.cpuconsumptiontime,
                     j.jobstatus as status,
                     j.assignedpriority,
                     j.currentpriority,
                     j.atlasrelease,
                     j.transformation,
                     j.homepackage,
                     j.resource_type,
                     j.starttime,
                     j.endtime,
                     j.creationtime,
                     j.hs06sec,
                     j.processingtype,
                     f.fsize as lib_size
                  FROM ATLAS_PANDA.JOBS_STATUSLOG s
                  INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 j ON (s.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.FILESTABLE4 f ON (j.jeditaskid = f.jeditaskid AND f.pandaid = j.pandaid)
                       INNER JOIN ATLAS_PANDA.TASK_ATTEMPTS ta ON (ta.jeditaskid = j.jeditaskid AND j.creationtime >= ta.starttime and j.modificationtime <= ta.endtime)
              WHERE
                  j.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
              AND j.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours / 24
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
                      inputfiletype,
                      inputfileproject,
                      corecount,
                      actualcorecount,
                      nevents,
                      cpuconsumptionunit,
                      cpuconsumptiontime,
                      status,
                      assignedpriority,
                      currentpriority,
                      atlasrelease,
                      transformation,
                      homepackage,
                      resource_type,
                      processingtype,
                      hs06sec,
                      starttime as execution_start_tstamp,
                      endtime as execution_end_tstamp,
                      completed_tstamp,
                      creationtime                                       as creation_tstamp,
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
                      inputfiletype,
                      inputfileproject,
                      corecount,
                      actualcorecount,
                      nevents,
                      cpuconsumptionunit,
                      cpuconsumptiontime,
                      status,
                      assignedpriority,
                      currentpriority,
                      atlasrelease,
                      transformation,
                      homepackage,
                      resource_type,
                      hs06sec,
                      processingtype,
                        starttime,
                        endtime,
                        completed_tstamp,
                        creationtime)
SELECT result.*
FROM result