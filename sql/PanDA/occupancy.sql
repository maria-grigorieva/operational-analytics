WITH all_statuses as (
SELECT NVL(running,0) as running,
       NVL(defined,0) as defined,
       NVL(assigned,0) as assigned,
       NVL(activated,0) as activated,
       NVL(starting,0) as starting,
       NVL(transferring,0) as transferring,
       computingsite as queue,
       TRUNC(sysdate) as datetime
FROM (
SELECT computingsite, jobstatus, count(pandaid) as n_jobs
FROM ATLAS_PANDA.JOBSACTIVE4
WHERE modificationtime >= sysdate - 1
GROUP BY computingsite, jobstatus
UNION ALL
(SELECT computingsite, jobstatus, count(pandaid) as n_jobs
    FROM ATLAS_PANDA.JOBSDEFINED4
WHERE modificationtime >= sysdate - 1
    GROUP BY computingsite, jobstatus
))
PIVOT
(
   SUM(n_jobs)
   for jobstatus in ('running' as running,
       'defined' as defined,
       'assigned' as assigned,
       'activated' as activated,
       'starting' as starting,
       'transferring' as transferring
))
        ORDER BY datetime, computingsite),
 rate as (
     SELECT datetime,
            queue,
      round(nvl((running+1)/((activated+assigned+defined+starting+10)*greatest(1,least(2,(assigned/nullif(activated,0))))),0),2) as queue_occupancy
      FROM all_statuses
 )
SELECT a.datetime,
       a.queue,
       a.running,
       (a.defined+a.assigned+a.activated+a.starting) as queued,
       a.transferring,
       r.queue_occupancy
FROM all_statuses a
    JOIN rate r ON (a.queue = r.queue)
ORDER BY r.queue_occupancy DESC