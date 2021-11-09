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
               (NVL(defined,0)+NVL(assigned,0)+NVL(activated,0)+NVL(starting,0)) as currently_queued,
               (NVL(finished,0)) as completed,
               computingsite as queue
        FROM (
        SELECT computingsite, jobstatus, count(pandaid) as n_jobs
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
        GROUP BY computingsite, jobstatus
        UNION ALL
        (SELECT computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
            GROUP BY computingsite, jobstatus
        )
        UNION ALL
        (SELECT computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
        and starttime >= sysdate - 1
            GROUP BY computingsite, jobstatus
        )
            UNION ALL
            (
            select computingsite, jobstatus, count(pandaid) as n_jobs
           from ATLAS_PANDA.JOBSWAITING4
           where modificationtime >= sysdate - 1
             and prodsourcelabel = 'user'
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
               'pending' as pending
        ))
                ORDER BY computingsite),
         totals as (
             SELECT SUM(running) as total_jobs_running,
                    SUM(completed) as total_jobs_completed,
                    SUM(currently_queued) as total_jobs_queued,
                    SUM(transferring) as total_jobs_transferring,
                    SUM(pending) as total_jobs_pending,
                    SUM(running)+SUM(completed)+SUM(currently_queued)+SUM(transferring)+SUM(pending) as total
              FROM all_statuses
         ),
        shares as (SELECT a.queue,
               round(NVL(a.running/NULLIF(t.total_jobs_running,0),0),6) as running_share,
               round(NVL(a.currently_queued/NULLIF(t.total_jobs_queued,0),0),6) as queued_share,
               round(NVL(a.completed/NULLIF(t.total_jobs_completed,0),0), 6) as completed_share,
               round(NVL(a.transferring/NULLIF(t.total_jobs_transferring,0),0), 6) as transferring_share,
               round(NVL(a.pending/NULLIF(t.total_jobs_pending,0),0), 6) as pending_share,
               round((a.running+a.currently_queued+a.completed+a.transferring+a.pending)/
                     (t.total),6) as total_share
        FROM all_statuses a, totals t),
     occupancy as (
         select
                queue,
               round(nvl((running+1)/((activated+assigned+defined+starting+10)*greatest(1,least(2,(assigned/nullif(activated,0))))),0),2) as queue_occupancy
         from all_statuses
         ),
     efficiency as (
         select
                queue,
                NVL(ROUND(finished / NULLIF((finished + failed),0), 4), 0) as queue_efficiency
         from all_statuses
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
                        PARTITION BY jeditaskid,queue ORDER BY modificationtime ASC) as prev_state,
                   ROUND((CAST(modificationtime as date) - (LAG(CAST(modificationtime as date), 1)
                   OVER (
                      PARTITION BY jeditaskid,queue ORDER BY modificationtime ASC))) *
                       60 * 60 * 24, 3) as lag
                    FROM ((SELECT ja4.computingsite as queue,
                                ja4.jeditaskid,
                                js.jobstatus,
                                js.modificationtime,
                                ja4.starttime
                    FROM ATLAS_PANDA.JOBS_STATUSLOG js
                         INNER JOIN ATLAS_PANDA.JOBSACTIVE4 ja4 ON (js.pandaid = ja4.pandaid)
                    WHERE js.modificationtime >= sysdate - 1
                            and js.prodsourcelabel = 'user'
                            and js.jobstatus in ('activated', 'running'))
                    UNION ALL
                    (SELECT ja4.computingsite as queue,
                           ja4.jeditaskid,
                           js.jobstatus,
                           js.modificationtime,
                           ja4.starttime
                    FROM ATLAS_PANDA.JOBS_STATUSLOG js
                         INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja4 ON (js.pandaid = ja4.pandaid)
                    WHERE js.modificationtime >= sysdate - 1
                          and js.prodsourcelabel = 'user'
                         and js.jobstatus in ('activated', 'running')
                        --and ja4.jobstatus = 'finished'
                    ))
              )
        where jobstatus = 'running'
        group by queue
     ),
    running_times as (
            select queue,
            round(avg(lead)) as running_time_avg,
            round(median(lead)) as running_time_median
            FROM (
                SELECT queue, jeditaskid, jobstatus, modificationtime, starttime, lead
                FROM (
                         SELECT ja4.computingsite                                            as                          queue,
                                ja4.jeditaskid,
                                js.jobstatus,
                                js.modificationtime                                          as                          modificationtime,
                                ja4.starttime,
                                LEAD(CAST(js.modificationtime as date), 1)
                                     OVER (
                                         PARTITION BY ja4.jeditaskid,ja4.computingsite ORDER BY js.modificationtime ASC) next_state_time,
                                ROUND((LEAD(CAST(js.modificationtime as date), 1)
                                            OVER (
                                                PARTITION BY ja4.jeditaskid,ja4.computingsite ORDER BY js.modificationtime ASC) -
                                       CAST(js.modificationtime as date)) * 60 * 60 * 24, 3) as                          lead
                         FROM ATLAS_PANDA.JOBS_STATUSLOG js
                                  INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja4 ON (js.pandaid = ja4.pandaid)
                         WHERE js.modificationtime >= sysdate - 1
                           and js.prodsourcelabel = 'user'
                           and js.jobstatus in ('running', 'finished')
                            and ja4.jobstatus = 'finished'
                     )
                    WHERE starttime >= sysdate -1
                 )
            WHERE jobstatus = 'running' and lead is not NULL and lead > 0
            group by queue
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
            als.currently_queued,
            als.pending,
            als.currently_queued+als.completed+als.running+als.transferring+als.pending as total_jobs,
            als.currently_queued+als.running as active_jobs,
            round(NVL((als.currently_queued)/NULLIF(als.completed,0),0),4) as queue_utilization,
            tt.total_time_avg,
            tt.total_time_median,
            rt.running_time_avg,
            rt.running_time_median,
            qt.queue_time_avg,
            qt.queue_time_max,
            qt.queue_time_median,
            qt.queue_time_min,
            round(NVL(als.currently_queued/NULLIF(als.running,0),0),4) as queue_filling,
            oc.queue_occupancy,
            ef.queue_efficiency,
            TRUNC(sysdate) as datetime
FROM queue_times qt, occupancy oc, efficiency ef,
     shares sh,
     all_statuses als, total_times tt,
     running_times rt
WHERE qt.queue = oc.queue
  and qt.queue = ef.queue
  and qt.queue = sh.queue
  and qt.queue = als.queue
  and qt.queue = tt.queue
  and qt.queue = rt.queue