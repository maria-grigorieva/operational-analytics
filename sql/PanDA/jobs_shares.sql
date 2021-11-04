WITH all_statuses as (
        SELECT NVL(running,0) as running,
               NVL(defined,0) as defined,
               NVL(assigned,0) as assigned,
               NVL(activated,0) as activated,
               NVL(starting,0) as starting,
               NVL(transferring,0) as transferring,
               NVL(failed,0) as failed,
               NVL(finished,0) as finished,
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
               'finished' as finished
        ))
                ORDER BY computingsite),
         totals as (
             SELECT SUM(running) as total_jobs_running,
                    SUM(completed) as total_jobs_completed,
                    SUM(queued) as total_jobs_queued,
                    SUM(running)+SUM(completed)+SUM(queued) as total
              FROM all_statuses
         ),
        shares as (SELECT a.queue,
               round(a.running/t.total_jobs_running,6) as running_share,
               round(a.queued/t.total_jobs_queued,6) as queued_share,
               round(a.completed/t.total_jobs_completed, 6) as completed_share,
               round((a.running+a.queued+a.completed)/
                     (t.total),6) as total_share
        FROM all_statuses a, totals t)
        select TRUNC(sysdate) as datetime,
               queue,
               running_share,
               queued_share,
               completed_share,
               total_share
               from shares
order by total_share desc