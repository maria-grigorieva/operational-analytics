SELECT queue,
       datetime,
       NVL(running,0) as running,
       NVL(queued,0) as queued,
       NVL(finished,0) as finished,
       NVL(failed,0) as failed,
       NVL(cancelled,0) as cancelled,
       NVL(closed,0) as closed,
       (NVL(finished,0)+NVL(failed,0)+NVL(cancelled,0)+NVL(closed,0)) as completed,
       round(NVL(queued/nullif(NVL(finished,0)+NVL(failed,0)+NVL(cancelled,0)+NVL(closed,0),0),0),4) as queue_utilization,
       round(NVL(queued/nullif(running,0),0),4) as queue_fullness,
       round(NVL(finished/nullif(NVL(finished,0)+NVL(failed,0)+NVL(cancelled,0)+NVL(closed,0),0),0),4) as queue_efficiency
       FROM (
                  with queued_jobs as (SELECT computingsite                   as queue,
                                              TRUNC(modificationtime, 'DD') as datetime,
                                              'queued'                        as status,
                                              NVL(count(distinct pandaid), 0) as n_jobs
                                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                                       WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                         AND prodsourcelabel = 'user'
                                         AND jobstatus in ('activated', 'defined', 'starting', 'assigned')
                                       GROUP BY computingsite, TRUNC(modificationtime, 'DD')),
                       running_jobs as (SELECT computingsite                   as queue,
                                               TRUNC(modificationtime, 'DD') as datetime,
                                               'running'                       as status,
                                               NVL(count(distinct pandaid), 0) as n_jobs
                                        FROM ATLAS_PANDA.JOBS_STATUSLOG
                                        WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                          AND prodsourcelabel = 'user'
                                          AND jobstatus = 'running'
                                        GROUP BY computingsite, TRUNC(modificationtime, 'DD')),
                       finished_jobs as (SELECT computingsite                   as queue,
                                                TRUNC(modificationtime, 'DD') as datetime,
                                                'finished'                      as status,
                                                NVL(count(distinct pandaid), 0) as n_jobs
                                         FROM ATLAS_PANDA.JOBS_STATUSLOG
                                         WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                           AND prodsourcelabel = 'user'
                                           AND jobstatus = 'finished'
                                         GROUP BY computingsite, TRUNC(modificationtime, 'DD')),
                       failed_jobs as (SELECT computingsite                   as queue,
                                              TRUNC(modificationtime, 'DD') as datetime,
                                              'failed'                        as status,
                                              NVL(count(distinct pandaid), 0) as n_jobs
                                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                                       WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                         AND prodsourcelabel = 'user'
                                         AND jobstatus = 'failed'
                                       GROUP BY computingsite, TRUNC(modificationtime, 'DD')),
                       closed_jobs as (SELECT computingsite                   as queue,
                                              TRUNC(modificationtime, 'DD') as datetime,
                                              'closed'                        as status,
                                              NVL(count(distinct pandaid), 0) as n_jobs
                                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                                       WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                         AND prodsourcelabel = 'user'
                                         AND jobstatus = 'closed'
                                       GROUP BY computingsite, TRUNC(modificationtime, 'DD')),
                       cancelled_jobs as (SELECT computingsite                   as queue,
                                                 TRUNC(modificationtime, 'DD') as datetime,
                                                 'cancelled'                     as status,
                                                 NVL(count(distinct pandaid), 0) as n_jobs
                                          FROM ATLAS_PANDA.JOBS_STATUSLOG
                                          WHERE modificationtime >= to_date(:datetime, 'yyyy-mm-dd')
                                            AND prodsourcelabel = 'user'
                                            AND jobstatus = 'cancelled'
                                          GROUP BY computingsite, TRUNC(modificationtime, 'DD'))
                  select *
                  FROM queued_jobs
                  UNION ALL
                  select *
                  from running_jobs
                  UNION ALL
                  select *
                  from finished_jobs
                  UNION ALL
                  select *
                  FROM failed_jobs
                  UNION ALL
                  select *
                  from closed_jobs
                  UNION ALL
                  select *
                  from cancelled_jobs
              )
PIVOT
        (
           SUM(n_jobs)
           for status in ('running' as running,
               'queued' as queued,
               'failed' as failed,
               'cancelled' as cancelled,
               'closed' as closed,
               'finished' as finished
        ))
    ORDER BY queue, datetime