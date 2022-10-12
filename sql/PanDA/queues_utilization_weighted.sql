with all_jobs as (
    SELECT distinct pandaid FROM ATLAS_PANDA.JOBS_STATUSLOG
    WHERE modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - :hours / 24)
      AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND prodsourcelabel = 'user'
),
statuses as (
    SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime,
           LEAD(CAST(s.modificationtime as date), 1)
                OVER (
                    PARTITION BY s.pandaid ORDER BY s.modificationtime ASC) as lead_timestamp,
           ROUND((LEAD(CAST(s.modificationtime as date), 1)
                       OVER (
                           PARTITION BY s.pandaid ORDER BY s.modificationtime ASC) -
                  CAST(s.modificationtime as date)) * 60 * 60 * 24, 3)       lead
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    INNER JOIN all_jobs a ON (s.pandaid = a.pandaid)
    WHERE s.jobstatus in ('pending', 'defined', 'assigned', 'activated', 'sent', 'starting',
                        'running', 'holding', 'transferring', 'merging',
                        'finished', 'failed', 'closed', 'cancelled')
),
    jobs as (
        SELECT (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - :hours / 24) as tstart,
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as tend,
               pandaid,
               queue,
               NVL(sum(activated_lead),0)+NVL(sum(sent_lead),0)+NVL(sum(starting_lead),0) as waiting_time,
               NVL(sum(running_lead),0) as running_time,
               NVL(sum(transferring_lead),0) as transferring_time,
               NVL(sum(merging_lead),0) as merging_time,
               NVL(sum(holding_lead),0) as holding_time,
               min(pending_first) as pending_timestamp,
               min(defined_first) as defined_timestamp,
               min(assigned_first) as assigned_timestamp,
               min(activated_first) as activated_timestamp,
               min(sent_first) as sent_timestamp,
               min(starting_first) as starting_timestamp,
               min(running_first) as running_timestamp,
               min(transferring_first) as transferring_timestamp,
               min(merging_first) as merging_timestamp,
               min(finished_first) as finished_timestamp,
               min(failed_first) as failed_timestamp,
               min(cancelled_first) as cancelled_timestamp,
               min(closed_first) as closed_timestamp,
               min(holding_first) as holding_timestamp,
               CASE WHEN finished_first is not None THEN 'finished'
                    WHEN failed_first is not None THEN 'failed'
                    WHEN cancelled_first is not None THEN 'cancelled'
                    WHEN closed_first is not None THEN 'closed'
               END as final_status
    FROM statuses
PIVOT (
                   min(modificationtime) as first,
                   sum(lead) as lead
                   FOR status
                   IN ('pending' AS pending,
                       'defined' AS defined,
                       'assigned' AS assigned,
                       'activated' AS activated,
                       'sent' AS sent,
                       'starting' AS starting,
                       'running' AS running,
                       'transferring' AS transferring,
                       'merging' AS merging,
                       'finished' AS finished,
                       'failed' AS failed,
                       'holding' AS holding,
                       'cancelled' as cancelled,
                       'closed' as closed
                       )
                   )
    GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - :hours / 24,
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
                pandaid, queue
        ORDER BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - :hours / 24,
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
pandaid, queue),
    result as (
        SELECT tstart,
               tend,
               pandaid,
               queue,
               final_status,
               waiting_time,
               running_time,
               merging_time,
               transferring_time,
               holding_time,
               activated_timestamp,
               running_timestamp,
               transferring_timestamp,
               merging_timestamp,
               COALESCE(finished_timestamp,
                        failed_timestamp,
                        cancelled_timestamp,
                        closed_timestamp) as completed_timestamp,
               CASE
                   WHEN activated_timestamp >= tstart and activated_timestamp < tend
                       THEN 1
                   WHEN pending_timestamp >= tstart and pending_timestamp < tend
                       THEN 1
                   WHEN defined_timestamp >= tstart and defined_timestamp < tend
                       THEN 1
                   WHEN assigned_timestamp >= tstart and assigned_timestamp < tend
                       THEN 1
                   WHEN sent_timestamp >= tstart and sent_timestamp < tend
                       THEN 1
                   WHEN starting_timestamp >= tstart and starting_timestamp < tend
                       THEN 1
                   ELSE 0
                   END                    as queued,
               CASE
                   WHEN running_timestamp >= tstart and running_timestamp < tend
                       THEN 1
                   ELSE 0
                   END                    as running,
               CASE
                   WHEN holding_timestamp >= tstart and holding_timestamp < tend
                       THEN 1
                   ELSE 0
                   END                    as holding,
               CASE
                   WHEN merging_timestamp >= tstart and merging_timestamp < tend
                       THEN 1
                   ELSE 0
                   END                    as merging,
               CASE
                   WHEN transferring_timestamp >= tstart and transferring_timestamp < tend
                       THEN 1
                   ELSE 0
                   END                    as transferring,
               CASE
                   WHEN COALESCE(finished_timestamp,
                                 failed_timestamp,
                                 cancelled_timestamp,
                                 closed_timestamp) >= tstart and COALESCE(finished_timestamp,
                                                                          failed_timestamp,
                                                                          cancelled_timestamp,
                                                                          closed_timestamp) < tend
                       THEN 1
                   ELSE 0
                   END                    as completed
        FROM jobs
    ),
     r1 as (
SELECT tstart,
       tend,
       queue,
       round(sum(running_time*queued),2) as running_queued,
       round(sum(running_time*running),2) as running_running,
       round(sum(running_time*holding),2) as running_holding,
       round(sum(running_time*completed),2) as running_completed,
       round(sum(running_time*merging),2) as running_merging,
       round(sum(running_time*transferring),2) as running_transferring,
       sum(queued) as n_queued,
       sum(running) as n_running,
       sum(completed) as n_completed,
       sum(merging) as n_merging,
       sum(transferring) as n_transferring,
       sum(holding) as n_holding,
       NVL(SELECT count(distinct pandaid) FROM result WHERE final_status='finished'),0) as n_finished,
       NVL(SELECT count(distinct pandaid) FROM result WHERE final_status='failed'),0) as n_failed,
       NVL(SELECT count(distinct pandaid) FROM result WHERE final_status='closed'),0) as n_closed,
       NVL(SELECT count(distinct pandaid) FROM result WHERE final_status='cancelled'),0) as n_cancelled,
       round(avg(running_time)) as av_running_time,
       round(avg(waiting_time)) as avg_waiting_time,
       round(avg(transferring_time)) as avg_transferring_time,
       round(avg(merging_time)) as avg_merging_time,
       round(avg(holding_time)) as avg_holding_time,
       count(distinct pandaid) as capacity,
       round(sum(running_time)) as capacity_weighted
FROM result
group by tstart,
       tend,
       queue)
SELECT tstart,
       tend,
       queue,
       running_queued,
       running_running,
       running_completed,
       running_merging,
       running_transferring,
       n_queued,
       n_running,
       n_completed,
       round(running_queued/nullif(running_queued + running_completed,0),2) as utilization_weighted,
       round(running_queued/nullif(running_queued + running_running + running_holding + running_merging + running_transferring,0), 2) as fullness_weighted,
       round(running_completed/nullif(running_completed + running_running + running_holding + running_merging + running_transferring,0), 2) as performance_weighted,
       round(n_queued/nullif(n_queued+n_completed,0),2) as utilization,
       round(n_queued/nullif(n_queued+n_running+n_merging+n_transferring+n_holding,0),2) as fullness,
       round(n_completed/nullif(n_completed+n_running+n_holding+n_transferring+n_merging,0),2) as performance,
       avg_waiting_time,
       avg_running_time,
       avg_merging_time,
       avg_transferring_time,
       avg_holding_time,
       capacity,
       capacity_weighted,
       n_finished/nullif((n_finished+n_failed+n_closed+n_cancelled),0) as efficiency
FROM r1