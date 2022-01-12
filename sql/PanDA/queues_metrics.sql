WITH all_statuses as (
        SELECT NVL(running,0) as running,
               NVL(defined,0) as defined,
               NVL(assigned,0) as assigned,
               NVL(activated,0) as activated,
               NVL(starting,0) as starting,
               NVL(transferring,0) as transferring,
               NVL(failed,0) as failed,
               NVL(finished,0) as finished,
               NVL(pending,0) as pending,
               (NVL(defined,0)+NVL(assigned,0)+NVL(activated,0)+NVL(starting,0)) as queued,
               (NVL(finished,0)) as completed,
               computingsite as queue
        FROM (
            (
                  SELECT computingsite, jobstatus, count(pandaid) as n_jobs
                  FROM ATLAS_PANDA.JOBSACTIVE4
                  WHERE modificationtime >= sysdate - 1 AND prodsourcelabel = 'user'
                  GROUP BY computingsite, jobstatus
              )
        UNION ALL
            (
                SELECT computingsite, jobstatus, count(pandaid) as n_jobs
                FROM ATLAS_PANDA.JOBSDEFINED4
                WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
                GROUP BY computingsite, jobstatus
            )
        UNION ALL
            (
                SELECT computingsite, jobstatus, count(pandaid) as n_jobs
                FROM ATLAS_PANDA.JOBSARCHIVED4
                WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user' and starttime >= sysdate - 1
                GROUP BY computingsite, jobstatus
            )
        UNION ALL
            (
                SELECT computingsite, jobstatus, count(pandaid) as n_jobs
                FROM ATLAS_PANDA.JOBSWAITING4
                WHERE modificationtime >= sysdate - 1 AND prodsourcelabel = 'user'
                GROUP BY computingsite, jobstatus
            )
        )
        PIVOT
        (
           SUM(n_jobs)
           for jobstatus in ('running' as running,
               'defined' as defined,
               'assigned' as assigned,
               'activated' as activated,
               'starting' as starting,
               'transferring' as transferring,
               'failed' as failed,
               'finished' as finished,
               'pending' as pending)
            )
            ORDER BY computingsite
        ),
        totals as (
             SELECT SUM(running) as total_jobs_running,
                    SUM(completed) as total_jobs_completed,
                    SUM(queued) as total_jobs_queued,
                    SUM(transferring) as total_jobs_transferring,
                    SUM(pending) as total_jobs_pending,
                    SUM(running)+SUM(completed)+SUM(queued)+SUM(transferring)+SUM(pending) as total
              FROM all_statuses
        ),
        shares as (
            SELECT a.queue,
               round(NVL(a.running/NULLIF(t.total_jobs_running,0),0),6) as running_share,
               round(NVL(a.queued/NULLIF(t.total_jobs_queued,0),0),6) as queued_share,
               round(NVL(a.completed/NULLIF(t.total_jobs_completed,0),0), 6) as completed_share,
               round(NVL(a.transferring/NULLIF(t.total_jobs_transferring,0),0), 6) as transferring_share,
               round(NVL(a.pending/NULLIF(t.total_jobs_pending,0),0), 6) as pending_share,
               round((a.running+a.queued+a.completed+a.transferring+a.pending)/
                     (t.total),6) as total_share
            FROM all_statuses a, totals t),
        queue_times as (
            SELECT queue,
                   round(avg(lag)) as queue_time_avg,
                   max(lag) as queue_time_max,
                   min(lag) as queue_time_min,
                   round(median(lag)) as queue_time_median
            FROM (
                SELECT queue,
                       jeditaskid,
                       jobstatus,
                       modificationtime,
                       LAG(CAST(modificationtime as date), 1)
                       OVER (
                            PARTITION BY jeditaskid,queue ORDER BY modificationtime ASC) as prev_state,
                       ROUND((CAST(modificationtime as date) - (LAG(CAST(modificationtime as date), 1)
                       OVER (
                          PARTITION BY jeditaskid,queue ORDER BY modificationtime ASC))) *
                           60 * 60 * 24, 3) as lag
                        FROM ((SELECT ja4.computingsite as queue,ja4.jeditaskid,js.jobstatus,js.modificationtime,ja4.starttime
                        FROM ATLAS_PANDA.JOBS_STATUSLOG js
                            INNER JOIN ATLAS_PANDA.JOBSACTIVE4 ja4 ON (js.pandaid = ja4.pandaid)
                        WHERE js.modificationtime >= sysdate - 1
                            AND js.prodsourcelabel = 'user' AND js.jobstatus in ('activated', 'running'))
                        UNION ALL
                        (
                            SELECT ja4.computingsite as queue,ja4.jeditaskid,js.jobstatus,js.modificationtime,ja4.starttime
                            FROM ATLAS_PANDA.JOBS_STATUSLOG js
                                INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja4 ON (js.pandaid = ja4.pandaid)
                            WHERE js.modificationtime >= sysdate - 1
                                AND js.prodsourcelabel = 'user' AND js.jobstatus IN ('activated', 'running')
                        ))
                  )
            WHERE jobstatus = 'running'
            GROUP BY queue
        ),
    running_times as (
            SELECT queue,
            round(avg(lead)) as running_time_avg,
            round(median(lead)) as running_time_median
            FROM (
                SELECT queue, jeditaskid, jobstatus, modificationtime, starttime, lead
                FROM (
                         SELECT ja4.computingsite as queue,
                                ja4.jeditaskid,
                                js.jobstatus,
                                js.modificationtime,
                                ja4.starttime,
                                LEAD(CAST(js.modificationtime as date), 1)
                                     OVER (
                                         PARTITION BY ja4.jeditaskid,ja4.computingsite ORDER BY js.modificationtime ASC) next_state_time,
                                ROUND((LEAD(CAST(js.modificationtime as date), 1)
                                            OVER (
                                                PARTITION BY ja4.jeditaskid,ja4.computingsite ORDER BY js.modificationtime ASC) -
                                       CAST(js.modificationtime as date)) * 60 * 60 * 24, 3) as lead
                         FROM ATLAS_PANDA.JOBS_STATUSLOG js
                                  INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja4 ON (js.pandaid = ja4.pandaid)
                         WHERE js.modificationtime >= sysdate - 1
                           AND js.prodsourcelabel = 'user'
                           AND js.jobstatus in ('running', 'finished')
                           AND ja4.jobstatus = 'finished'
                     )
                    WHERE starttime >= sysdate -1
                 )
            WHERE jobstatus = 'running' AND lead is not NULL AND lead > 0
            GROUP BY queue
     ),
     total_times as (
        SELECT computingsite as queue,
           round(avg((endtime-starttime)*60*60*24)) as total_time_avg,
           round(median((endtime-starttime)*60*60*24)) as total_time_median
         FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE modificationtime >= sysdate - 1
          and prodsourcelabel = 'user'
          and jobstatus in ('finished')
          and starttime >= sysdate - 1
        group by computingsite
     )
     SELECT als.queue,
            CASE WHEN sh.queue LIKE '%_VP%' THEN 'VP'
                WHEN sh.queue LIKE '%_TEST%' THEN 'TEST'
                WHEN sh.queue LIKE '%_LAKE%' then 'LAKE'
                WHEN sh.queue LIKE '%_HEP%' THEN 'HEP'
                END queue_type,
            sh.running_share,
            sh.queued_share,
            sh.completed_share,
            sh.transferring_share,
            sh.pending_share,
            sh.total_share,
            als.running,
            als.transferring,
            als.completed,
            als.queued,
            als.pending,
            als.activated,
            als.defined,
            als.starting,
            als.assigned,
            als.queued+als.completed+als.running+als.transferring+als.pending as total_jobs,
            round(NVL((als.queued)/NULLIF(als.completed,0),als.queued),4) as queue_utilization,
            round(nvl((als.running+1)/((als.activated+als.assigned+als.defined+als.starting+10)*greatest(1,least(2,(als.assigned/nullif(als.activated,0))))),0),4) as queue_occupancy,
            NVL(ROUND(als.finished/NULLIF((als.finished+als.failed),0),4),0) as queue_efficiency,
            round(NVL(als.queued/NULLIF(als.running,0),als.queued),4) as queue_fullness,
            tt.total_time_avg,
            tt.total_time_median,
            rt.running_time_avg,
            rt.running_time_median,
            qt.queue_time_avg,
            qt.queue_time_max,
            qt.queue_time_median,
            qt.queue_time_min,
            TRUNC(sysdate,'HH24') as datetime
FROM queue_times qt,shares sh,all_statuses als,total_times tt,running_times rt
WHERE qt.queue = sh.queue AND qt.queue = als.queue AND qt.queue = tt.queue AND qt.queue = rt.queue