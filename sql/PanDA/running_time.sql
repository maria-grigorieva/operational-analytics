SELECT
       queue,
       trunc(modificationtime, 'DD') as datetime,
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
                        WHERE modificationtime >= to_date(:datetime, 'YYYY-MM-DD')
                          AND prodsourcelabel = 'user'
                          AND jobstatus in ('running', 'finished'))
              )
WHERE jobstatus = 'running' and lead is not Null
GROUP BY queue,trunc(modificationtime, 'DD')