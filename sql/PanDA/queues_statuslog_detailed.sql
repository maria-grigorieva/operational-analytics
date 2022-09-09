with statuses as (
        SELECT s.pandaid,
            s.computingsite                   as queue,
         'queued'                          as status
  FROM ATLAS_PANDA.JOBS_STATUSLOG s
  WHERE s.modificationtime >=
        trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440 - (:min / 1440)
    AND s.modificationtime < trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440
    AND s.jobstatus in ('activated', 'defined', 'starting', 'assigned')
    UNION ALL
        SELECT s.pandaid,
            s.computingsite                   as queue,
         s.jobstatus as status
  FROM ATLAS_PANDA.JOBS_STATUSLOG s
  WHERE s.modificationtime >=
        trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440 - (:min / 1440)
    AND s.modificationtime < trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440
    AND s.jobstatus in
        ('running', 'finished', 'failed', 'closed', 'cancelled', 'transferring')
    ),
    jobs as (
    SELECT j.computingsite                   as queue,
           j.cpuconsumptionunit,
           s.status,
           NVL(count(distinct s.pandaid), 0) as n_jobs
  FROM ATLAS_PANDAARCH.JOBSARCHIVED j
  INNER JOIN statuses s ON (j.pandaid = s.pandaid)
    WHERE j.statechangetime <= trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440
  GROUP BY j.computingsite, j.cpuconsumptionunit, s.status
  UNION ALL
      SELECT j.computingsite                   as queue,
           j.cpuconsumptionunit,
            s.status,
           NVL(count(distinct s.pandaid), 0) as n_jobs
  FROM ATLAS_PANDA.JOBSARCHIVED4 j
    INNER JOIN statuses s ON (j.pandaid = s.pandaid)
    WHERE j.statechangetime <= trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440
    GROUP BY j.computingsite, j.cpuconsumptionunit, s.status
)
    SELECT queue,
     cpuconsumptionunit,
     trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440                      as datetime,
     NVL(running, 0)                                                          as running,
     NVL(queued, 0)                                                           as queued,
     NVL(finished, 0)                                                         as finished,
     NVL(failed, 0)                                                           as failed,
     NVL(cancelled, 0)                                                        as cancelled,
     NVL(closed, 0)                                                           as closed,
     NVL(transferring, 0)                                                     as transferring,
     (NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0)) as completed,
     round(NVL(queued /
               nullif(NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0),
                      0), 0),
           4)                                                                 as queue_utilization,
     round(NVL(queued / nullif(running, 0), 0), 4)                            as queue_fullness,
     round(NVL(finished /
               nullif(NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0),
                      0), 0),
           4)                                                                 as queue_efficiency
    FROM jobs
PIVOT
(
SUM(n_jobs)
for status in ('running' as running,
  'queued' as queued,
  'failed' as failed,
  'cancelled' as cancelled,
  'closed' as closed,
  'finished' as finished,
  'transferring' as transferring
  ))
ORDER BY queue, cpuconsumptionunit, trunc(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'hh') + floor(to_char(to_date(:now, 'YYYY-MM-DD HH24:MI:SS'), 'mi')/:min)*:min/1440