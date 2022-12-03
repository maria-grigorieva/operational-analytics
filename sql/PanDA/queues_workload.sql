with jobs_statuses as (SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    WHERE s.modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
      AND s.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND s.prodsourcelabel = 'user'
    ),
    prev as (
        SELECT pandaid,
               queue,
               status,
               modificationtime
               FROM (SELECT pandaid,
                            computingsite                                                            as queue,
                            jobstatus                                                                as status,
                              (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24) as modificationtime,
                              ROW_NUMBER() OVER (PARTITION BY pandaid ORDER BY modificationtime desc) AS rn
                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                       WHERE pandaid in (SELECT distinct pandaid from jobs_statuses)
                          AND modificationtime < (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24))
        WHERE rn in 1),
    merge as (SELECT pandaid,
                     queue,
                     status,
                     modificationtime,
                     NVL(LEAD(CAST(modificationtime as date), 1)
                              OVER (
                                  PARTITION BY pandaid ORDER BY modificationtime ASC),
                         trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')) as lead_timestamp
              FROM (SELECT *
                    FROM jobs_statuses
                    UNION ALL
                    SELECT *
                    FROM prev)
              ),
    queued_jobs as (
        SELECT distinct pandaid, queue
        FROM merge WHERE status in ('pending','defined','assigned','activated', 'throttled', 'sent', 'starting')
    ),
    executing_jobs as (
        SELECT distinct pandaid, queue
        FROM merge WHERE status in ('running','holding','merging','transferring')
    ),
    completed_jobs as (
                SELECT distinct pandaid, queue
        FROM merge WHERE status in ('finished','failed','closed','cancelled')
    ),
    finished_jobs as (
                        SELECT distinct pandaid, queue
        FROM merge WHERE status = 'finished'
    ),
    failed_jobs as (
                        SELECT distinct pandaid, queue
        FROM merge WHERE status in ('failed','closed','cancelled')
    ),
    jobs as (
        SELECT pandaid,
               queue,
                        max(inputfilebytes) as inputfilebytes,
                        max(outputfilebytes) as outputfilebytes from (SELECT pandaid,
                                                                              computingsite           as queue,
                                                                              NVL(inputfilebytes, 0)  as inputfilebytes,
                                                                              NVL(outputfilebytes, 0) as outputfilebytes
                                                                       FROM ATLAS_PANDA.JOBSARCHIVED4
                                                                       WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                                                                       UNION ALL
                                                                       SELECT pandaid,
                                                                              computingsite           as queue,
                                                                              NVL(inputfilebytes, 0)  as inputfilebytes,
                                                                              NVL(outputfilebytes, 0) as outputfilebytes
                                                                       FROM ATLAS_PANDAARCH.JOBSARCHIVED
                                                                       WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                                                                       UNION ALL
                                                                       SELECT pandaid,
                                                                              computingsite           as queue,
                                                                              NVL(inputfilebytes, 0)  as inputfilebytes,
                                                                              NVL(outputfilebytes, 0) as outputfilebytes
                                                                       FROM ATLAS_PANDA.JOBSACTIVE4
                                                                       WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                                                                       UNION ALL
                                                                       SELECT pandaid,
                                                                              computingsite           as queue,
                                                                              NVL(inputfilebytes, 0)  as inputfilebytes,
                                                                              NVL(outputfilebytes, 0) as outputfilebytes
                                                                       FROM ATLAS_PANDA.JOBSWAITING4
                                                                       WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                                                                       UNION ALL
                                                                       SELECT pandaid,
                                                                              computingsite           as queue,
                                                                              NVL(inputfilebytes, 0)  as inputfilebytes,
                                                                              NVL(outputfilebytes, 0) as outputfilebytes
                                                                       FROM ATLAS_PANDA.JOBSDEFINED4
                                                                       WHERE pandaid IN (SELECT distinct pandaid FROM merge))
                                                                group by pandaid, queue
    ),
    executing_metrics as (
        SELECT ej.queue, count(ej.pandaid) as n_executing_jobs,
            sum(j.inputfilebytes) as executing_input_volume,
            sum(j.outputfilebytes) as executing_output_volume
        FROM jobs j INNER JOIN executing_jobs ej  on (j.pandaid = ej.pandaid)
        group by ej.queue
    ),
    queued_metrics as (
        SELECT qj.queue, count(qj.pandaid) as n_queued_jobs,
            sum(j.inputfilebytes) as queued_input_volume,
            sum(j.outputfilebytes) as queued_output_volume
        FROM jobs j INNER JOIN queued_jobs qj  on (j.pandaid = qj.pandaid)
        group by qj.queue
    ),
    completed_metrics as (
        SELECT cj.queue,
               count(cj.pandaid) as n_completed_jobs,
               sum(j.inputfilebytes) as completed_input_volume,
               sum(j.outputfilebytes) as completed_output_volume
        FROM jobs j INNER JOIN completed_jobs cj  on (j.pandaid = cj.pandaid)
        GROUP BY cj.queue
    ),
    finished_metrics as (
               SELECT cj.queue,
               count(cj.pandaid) as n_finished_jobs,
               sum(j.inputfilebytes) as finished_input_volume,
               sum(j.outputfilebytes) as finished_output_volume
        FROM jobs j INNER JOIN finished_jobs cj  on (j.pandaid = cj.pandaid)
        GROUP BY cj.queue
    ),
    failed_metrics as (
                       SELECT cj.queue,
               count(cj.pandaid) as n_failed_jobs,
               sum(j.inputfilebytes) as failed_input_volume,
               sum(j.outputfilebytes) as failed_output_volume
        FROM jobs j INNER JOIN failed_jobs cj  on (j.pandaid = cj.pandaid)
        GROUP BY cj.queue
    ),
    total_metrics as (
        SELECT
            queue,
            count(pandaid) as n_total_jobs
        from jobs
         group by queue
    ),
    volumes as (SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
                       t.queue,
                       NVL(sum(qj.n_queued_jobs),0) as n_queued_jobs,
                       NVL(sum(qj.queued_input_volume),0) as queued_input_volume,
                       NVL(sum(qj.queued_output_volume),0) as queued_output_volume,
                       NVL(sum(ex.n_executing_jobs),0) as n_executing_jobs,
                       NVL(sum(ex.executing_input_volume),0) as executing_input_volume,
                      NVL(sum(ex.executing_output_volume),0) as executing_output_volume,
                       NVL(sum(c.n_completed_jobs),0)                                     as n_completed_jobs,
                       NVL(sum(c.completed_input_volume),0)                               as completed_input_volume,
                       NVL(sum(c.completed_output_volume),0)                              as completed_output_volume,
                       NVL(sum(fi.n_finished_jobs),0)                                     as n_finished_jobs,
                       NVL(sum(fi.finished_input_volume),0)                               as finished_input_volume,
                       NVL(sum(fi.finished_output_volume),0)                              as finished_output_volume,
                       NVL(sum(fa.n_failed_jobs),0)                                       as n_failed_jobs,
                       NVL(sum(fa.failed_input_volume),0)                                 as failed_input_volume,
                       NVL(sum(fa.failed_output_volume),0)                                as failed_output_volume,
                       NVL(sum(t.n_total_jobs),0)                                         as total_n_jobs
                FROM total_metrics t
                         LEFT OUTER JOIN completed_metrics c on (t.queue = c.queue)
                         LEFT OUTER JOIN finished_metrics fi on (t.queue = fi.queue)
                         LEFT OUTER JOIN failed_metrics fa on (t.queue = fa.queue)
                         LEFT OUTER JOIN executing_metrics ex on (t.queue = ex.queue)
                         LEFT OUTER JOIN queued_metrics qj on (t.queue = qj.queue)
                GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
                         t.queue),
    timings as (SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
                    queue,
                       round(avg(executing_time))                                       as avg_executing_time,
                       round(avg(waiting_time))                                         as avg_waiting_time,
                       round(median(executing_time))                                    as median_executing_time,
                       round(median(waiting_time))                                      as median_waiting_time,
                       sum(finished)                                                    as n_finished_jobs,
                       sum(failed)                                                      as n_failed_jobs,
                       (sum(finished) + sum(failed))                                    as n_total_jobs,
                       NVL(sum(finished) / nullif((sum(finished) + sum(failed)), 0), 0) as efficiency
                FROM (SELECT pandaid,
                             queue,
                             GREATEST(round((GREATEST(NVL(max(running_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(merging_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(holding_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(transferring_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24))) -
                                             LEAST(NVL(min(running_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(merging_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(holding_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(transferring_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')))) *
                                            60 * 60 *
                                            24), 0) as executing_time,
                             GREATEST(round((GREATEST(NVL(max(pending_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(defined_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(activated_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(assigned_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(sent_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(starting_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)),
                                                      NVL(max(throttled_lead_timestamp),
                                                          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24))) -
                                             LEAST(NVL(min(pending_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(defined_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(assigned_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(activated_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(sent_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(starting_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')),
                                                   NVL(min(throttled_modificationtime),
                                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')))) *
                                            60 *
                                            60 * 24),
                                      0)            as waiting_time,
                             CASE
                                 WHEN max(finished_modificationtime) <
                                      trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                                     THEN 1
                                 ELSE 0
                                 END                as finished,
                             CASE
                                 WHEN COALESCE(max(failed_modificationtime),
                                               max(closed_modificationtime),
                                               max(cancelled_modificationtime)) <
                                      trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                                     THEN 1
                                 ELSE 0
                                 END                as failed
                      FROM merge
                          PIVOT (
                          min(modificationtime) as modificationtime,
                              max(lead_timestamp) as lead_timestamp
                          FOR status
                          IN ('pending' AS pending,
                              'defined' AS defined,
                              'assigned' AS assigned,
                              'activated' AS activated,
                              'sent' AS sent,
                              'starting' AS starting,
                              'throttled' as throttled,
                              'running' AS running,
                              'merging' AS merging,
                              'holding' AS holding,
                              'transferring' AS transferring,
                              'finished' as finished,
                              'failed' as failed,
                              'closed' as closed,
                              'cancelled' as cancelled
                              )
                          )
                      GROUP BY pandaid, queue)
                GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
                         queue)
SELECT v.datetime,
       v.queue,
       v.n_queued_jobs,
       v.queued_input_volume,
       v.queued_output_volume,
       v.n_executing_jobs,
       v.executing_input_volume,
       v.executing_output_volume,
       v.n_completed_jobs,
       v.completed_input_volume,
       v.completed_output_volume,
       v.n_finished_jobs,
       v.finished_input_volume,
       v.finished_output_volume,
       v.n_failed_jobs,
       v.failed_input_volume,
       v.failed_output_volume,
       v.total_n_jobs,
       t.avg_executing_time,
       t.avg_waiting_time,
       t.median_executing_time,
       t.median_waiting_time,
       t.n_finished_jobs,
       t.n_failed_jobs,
       t.n_total_jobs,
       round(t.efficiency,4)  as efficiency,
       round(NVL(v.n_queued_jobs/nullif(v.n_completed_jobs,0),v.n_queued_jobs),4) as utilization,
       round(NVL((v.queued_input_volume/1000000000000)/nullif(v.completed_input_volume/1000000000000,0),v.queued_input_volume/1000000000000),4) as utilization_weighted,
       round(NVL(v.n_executing_jobs/nullif(v.n_completed_jobs,0),v.n_executing_jobs),4) as fullness,
       round(NVL((v.executing_input_volume/1000000000000)/nullif(v.completed_input_volume/1000000000000,0),v.executing_input_volume/1000000000000),4) as fullness_weighted,
       round(NVL((v.finished_input_volume/1000000000000)/nullif(v.completed_input_volume/1000000000000,0),0),4) as efficiency_weighted
FROM volumes v, timings t
WHERE v.queue = t.queue