SELECT queue,
       TRUNC(from_time,'HH24') as start_time,
       TRUNC(to_time,'HH24') as end_time,
       NVL(running, 0)                                                          as running,
       NVL(queued, 0)                                                           as queued,
       NVL(finished, 0)                                                         as finished,
       NVL(failed, 0)                                                           as failed,
       NVL(cancelled, 0)                                                        as cancelled,
       NVL(closed, 0)                                                           as closed,
       NVL(transferring, 0)                                                     as transferring,
       (NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0)) as completed,
       round(NVL(queued / nullif(NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0), 0), 0),
             4)                                                                 as queue_utilization,
       round(NVL(queued / nullif(running, 0), 0), 4)                            as queue_fullness,
       round(NVL(finished / nullif(NVL(finished, 0) + NVL(failed, 0) + NVL(cancelled, 0) + NVL(closed, 0), 0), 0),
             4)                                                                 as queue_efficiency
    FROM (
        (SELECT computingsite                                           as queue,
                to_date(:from_time, 'YYYY-MM-DD HH24:MI:SS') as from_time,
                to_date(:to_time, 'YYYY-MM-DD HH24:MI:SS') as to_time,
                'queued'                                                as status,
                NVL(count(distinct pandaid), 0)                         as n_jobs
         FROM ATLAS_PANDA.JOBS_STATUSLOG
         WHERE modificationtime >= to_date(:from_time, 'YYYY-MM-DD HH24:MI:SS')
           AND modificationtime < to_date(:to_time, 'YYYY-MM-DD HH24:MI:SS')
           AND prodsourcelabel = 'user'
           AND jobstatus in ('activated', 'defined', 'starting', 'assigned')
         GROUP BY computingsite
        )
        UNION ALL
        (SELECT computingsite                                           as queue,
                to_date(:from_time, 'YYYY-MM-DD HH24:MI:SS') as from_time,
                to_date(:to_time, 'YYYY-MM-DD HH24:MI:SS') as to_time,
                jobstatus,
                NVL(count(distinct pandaid), 0)                         as n_jobs
         FROM ATLAS_PANDA.JOBS_STATUSLOG
         WHERE modificationtime >= to_date(:from_time, 'YYYY-MM-DD HH24:MI:SS')
           AND modificationtime < to_date(:to_time, 'YYYY-MM-DD HH24:MI:SS')
           AND prodsourcelabel = 'user'
           AND jobstatus in ('running', 'finished', 'failed', 'closed', 'cancelled', 'transferring')
         GROUP BY computingsite, jobstatus
        )
    ) PIVOT
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
    ORDER BY queue
