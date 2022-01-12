SELECT queue,
       trunc(modificationtime, 'DD') as datetime,
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
                WHERE modificationtime >= to_date(:datetime, 'YYYY-MM-DD')
                  AND prodsourcelabel = 'user'
                  AND jobstatus in ('activated', 'running')
              )
       )
WHERE jobstatus = 'running' and lag is not Null
GROUP BY queue, trunc(modificationtime, 'DD')