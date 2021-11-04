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
               (NVL(failed,0)+NVL(finished,0)) as completed,
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
            GROUP BY computingsite, jobstatus
        )
            UNION ALL
        (SELECT computingsite, jobstatus, count(pandaid) as n_jobs
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
        WHERE modificationtime >= sysdate - 1 and prodsourcelabel = 'user'
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
                    SUM(queued) as total_jobs_queued,
                    SUM(transferring) as total_jobs_transferring,
                    SUM(pending) as total_jobs_pending,
                    SUM(running)+SUM(completed)+SUM(queued)+SUM(transferring)+SUM(pending) as total
              FROM all_statuses
         ),
        shares as (SELECT a.queue,
               round(NVL(a.running/NULLIF(t.total_jobs_running,0),0),6) as running_share,
               round(NVL(a.queued/NULLIF(t.total_jobs_queued,0),0),6) as queued_share,
               round(NVL(a.completed/NULLIF(t.total_jobs_completed,0),0), 6) as completed_share,
               round(NVL(a.transferring/NULLIF(t.total_jobs_transferring,0),0), 6) as transferring_share,
               round(NVL(a.pending/NULLIF(t.total_jobs_pending,0),0), 6) as pending_share,
               round((a.running+a.queued+a.completed+a.transferring+a.pending)/
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
     )
     SELECT sh.queue,
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
            als.queued+als.completed+als.running+als.transferring+als.pending as total_jobs,
            round(NVL(als.queued/NULLIF(als.completed,0),0),4) as queue_service_quality,
            qt.queue_time_avg,
            qt.queue_time_median,
            qt.queue_time_min,
            qt.queue_time_max,
            round(NVL(als.queued/NULLIF(als.running,0),0),4) as queue_filling,
            oc.queue_occupancy,
            ef.queue_efficiency,
            TRUNC(sysdate) as datetime
FROM queue_times qt, occupancy oc, efficiency ef,
     shares sh, all_statuses als
WHERE qt.queue = oc.queue
  and qt.queue = ef.queue
  and qt.queue = sh.queue
  and qt.queue = als.queue