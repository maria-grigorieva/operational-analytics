with arrival_rates as (
    SELECT queue, count(distinct pandaid) as queued
FROM (
    SELECT computingsite as queue,
        pandaid
    FROM ATLAS_PANDA.JOBSACTIVE4
    WHERE modificationtime >= sysdate - 1
    AND jobstatus in ('defined','activated','assigned','starting')
    AND prodsourcelabel = 'user'
            UNION ALL
        SELECT computingsite as queue,
           pandaid
        FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE modificationtime >= sysdate - 1
        AND jobstatus in ('defined','activated','assigned','starting')
    AND prodsourcelabel = 'user'
)
    group by queue
    ),
     service_rates as (
        SELECT computingsite as queue,
           count(pandaid) as completed
        FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE modificationtime >= sysdate - 1
        AND jobstatus in ('finished','failed','closed','cancelled')
        AND prodsourcelabel = 'user'
        GROUP BY computingsite
     ),
     queue_times as (
           select queue,
       round(avg(lag)) as queue_time_avg,
       max(lag) as queue_time_max,
       min(lag) as queue_time_min,
       round(median(lag)) as queue_time_median
       from (
                  select queue,
                         jeditaskid,
                         jobstatus,
                         modificationtime,
                         LAG(CAST(modificationtime as date), 1)
                             OVER (
                                 PARTITION BY jeditaskid ORDER BY modificationtime ASC) as prev_state,
                         ROUND((CAST(modificationtime as date) - (LAG(CAST(modificationtime as date), 1)
                                                                      OVER (
                                                                          PARTITION BY jeditaskid ORDER BY modificationtime ASC))) *
                               60 * 60 * 24, 3)                                         as lag
                  FROM (SELECT ja4.computingsite as queue,
                               ja4.jeditaskid,
                               js.jobstatus,
                               min(js.modificationtime) as modificationtime
                        FROM ATLAS_PANDA.JOBS_STATUSLOG js
                                 INNER JOIN ATLAS_PANDA.JOBSACTIVE4 ja4 ON (js.pandaid = ja4.pandaid)
                        WHERE js.modificationtime >= sysdate - 1
                          and js.prodsourcelabel = 'user'
                          and js.jobstatus in ('activated', 'running')
                        group by ja4.computingsite,
                                 ja4.jeditaskid,
                                 js.jobstatus)
              )
where jobstatus = 'running'
group by queue
     ),
    volumes as (
        select queue,
               count(distinct pandaid) as queue_volume from (
                                           select computingsite  as queue, pandaid
                                           from ATLAS_PANDA.JOBSACTIVE4
                                           where modificationtime >= sysdate - 1
                                             and prodsourcelabel = 'user'
                                           UNION ALL
                                           select computingsite  as queue, pandaid
                                           from ATLAS_PANDA.JOBSDEFINED4
                                           where modificationtime >= sysdate - 1
                                             and prodsourcelabel = 'user'
                                           UNION ALL
                                           select computingsite  as queue, pandaid
                                           from ATLAS_PANDA.JOBSARCHIVED4
                                           where modificationtime >= sysdate - 1
                                             and prodsourcelabel = 'user'
                                            UNION ALL
                                            select computingsite  as queue, pandaid
                                           from ATLAS_PANDA.JOBSWAITING4
                                           where modificationtime >= sysdate - 1
                                             and prodsourcelabel = 'user'
                                       )
        group by queue
     ),
     jobs_running as (
         SELECT computingsite as queue,
       count(pandaid) as jobs_running
    FROM ATLAS_PANDA.JOBSACTIVE4
    WHERE modificationtime >= sysdate - 1
    AND jobstatus='running'
         and prodsourcelabel = 'user'
         group by computingsite
     )
SELECT ar.queue, ar.queued, sr.completed,
       round(ar.queued/sr.completed,4) as queue_service_quality,
       qt.queue_time_avg,
       qt.queue_time_median,
       t.queue_volume,
       round(ar.queued/jr.jobs_running,4) as queue_filling,
       jr.jobs_running,
                  TRUNC(sysdate) as datetime
FROM arrival_rates ar, service_rates sr, queue_times qt, volumes t, jobs_running jr
WHERE ar.queue = sr.queue and qt.queue = ar.queue and t.queue = ar.queue
and jr.queue = ar.queue


