with statuses as (
    SELECT queue,
           datetime,
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
     (SELECT computingsite                   as queue,
                                 TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')   as datetime,
                                 'queued'                        as status,
                                 NVL(count(distinct pandaid), 0) as n_jobs
              FROM ATLAS_PANDA.JOBS_STATUSLOG
              WHERE modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
                AND modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
                AND prodsourcelabel = 'user'
                AND jobstatus in ('activated', 'defined', 'starting', 'assigned')
              GROUP BY computingsite
         )
         UNION ALL
        (SELECT computingsite                   as queue,
                                  TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')   as datetime,
                                  jobstatus,
                                  NVL(count(distinct pandaid), 0) as n_jobs
               FROM ATLAS_PANDA.JOBS_STATUSLOG
               WHERE modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
                 AND modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
                 AND prodsourcelabel = 'user'
                 AND jobstatus in ('running','finished','failed','closed','cancelled','transferring')
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
    ORDER BY queue, datetime
),
     queue_time as (
         SELECT queue,
               trunc(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD') as datetime,
               NVL(round(avg(lag)),0) as avg_queue_time,
               NVL(max(lag),0) as max_queue_time,
               NVL(min(lag),0) as min_queue_time,
               NVL(round(median(lag)),0) as median_queue_time,
               NVL(round(stats_mode(lag)),0) as mode_queue_time,
               NVL(round(stddev(lag)),0) as stddev_queue_time
        FROM (
                 SELECT queue,
                        pandaid,
                        jobstatus,
                        modificationtime,
                        LAG(CAST(modificationtime as date), 1)
                            OVER (
                                PARTITION BY pandaid,queue ORDER BY modificationtime ASC) as prev_state,
                        ROUND((CAST(modificationtime as date) - (LAG(CAST(modificationtime as date), 1)
                                                                     OVER (
                                                                         PARTITION BY pandaid,queue ORDER BY modificationtime ASC))) *
                              60 * 60 * 24, 3)                                            as lag
                             FROM (SELECT pandaid, computingsite as queue, jobstatus, modificationtime
                                    FROM ATLAS_PANDA.JOBS_STATUSLOG
                                   WHERE modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
                                     AND modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
                                      AND prodsourcelabel = 'user'
                                      AND jobstatus in ('activated', 'running')
                                  )
                           )
                    WHERE jobstatus = 'running' and lag is not Null
                    GROUP BY queue
     ),
     running_time as (
         SELECT
           queue,
           trunc(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD') as datetime,
           NVL(round(avg(lead)),0) as avg_running_time,
           NVL(max(lead),0) as max_running_time,
           NVL(min(lead),0) as min_running_time,
           NVL(round(median(lead)),0) as median_running_time,
           NVL(round(stats_mode(lead)),0) as mode_running_time,
           NVL(round(stddev(lead)),0) as stddev_running_time
         FROM (
              SELECT queue,
                     pandaid,
                     jobstatus,
                     modificationtime,
                     LEAD(CAST(modificationtime as date), 1)
                          OVER (
                              PARTITION BY pandaid,queue ORDER BY modificationtime ASC) next_state_time,
                     ROUND((LEAD(CAST(modificationtime as date), 1)
                                 OVER (
                                     PARTITION BY pandaid,queue ORDER BY modificationtime ASC) -
                            CAST(modificationtime as date)) * 60 * 60 * 24, 3) as       lead
              FROM (SELECT pandaid, computingsite as queue, jobstatus, modificationtime
                    FROM ATLAS_PANDA.JOBS_STATUSLOG
                      WHERE modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
                      AND modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
                      AND prodsourcelabel = 'user'
                      AND jobstatus in ('running', 'finished'))
                              )
                WHERE jobstatus = 'running' and lead is not Null
                GROUP BY queue
     )
SELECT s.*,
       qt.avg_queue_time,
       qt.max_queue_time,
       qt.min_queue_time,
       qt.median_queue_time,
       qt.mode_queue_time,
       rt.avg_running_time,
       rt.max_running_time,
       rt.min_running_time,
       rt.median_running_time,
       rt.mode_running_time
FROM statuses s
INNER JOIN queue_time qt ON (s.queue = qt.queue and s.datetime = qt.datetime)
INNER JOIN running_time rt ON (s.queue = rt.queue and s.datetime = qt.datetime)
