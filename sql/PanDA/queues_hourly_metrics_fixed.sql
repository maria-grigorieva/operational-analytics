with statuses as (
    SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime,
            NVL(LEAD(CAST(s.modificationtime as date), 1)
                OVER (
                    PARTITION BY s.pandaid ORDER BY s.modificationtime ASC),
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')) as lead_timestamp
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    WHERE s.modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
      AND s.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND s.prodsourcelabel = 'user'),
    jobs as (
        SELECT pandaid,
                     NVL(inputfilebytes, 0)  as inputfilebytes,
                     NVL(outputfilebytes, 0) as outputfilebytes
              FROM ATLAS_PANDA.JOBSARCHIVED4
              WHERE pandaid in (SELECT distinct pandaid FROM statuses)
                AND modificationtime >=
                    (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)
                AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
              UNION ALL
              SELECT pandaid,
                     NVL(inputfilebytes, 0)  as inputfilebytes,
                     NVL(outputfilebytes, 0) as outputfilebytes
              FROM ATLAS_PANDAARCH.JOBSARCHIVED
              WHERE pandaid in (SELECT distinct pandaid FROM statuses)
                AND prodsourcelabel = 'user'
                AND modificationtime >=
                    (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)
                AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
              UNION ALL
              SELECT pandaid,
                     NVL(inputfilebytes, 0) as inputfilebytes,
                     0                      as outputfilebytes
              FROM ATLAS_PANDA.JOBSACTIVE4
              WHERE pandaid in (SELECT distinct pandaid FROM statuses)
                AND prodsourcelabel = 'user'
              UNION ALL
              SELECT pandaid,
                     NVL(inputfilebytes, 0) as inputfilebytes,
                     0                      as outputfilebytes
              FROM ATLAS_PANDA.JOBSWAITING4
              WHERE pandaid in (SELECT distinct pandaid FROM statuses)
                AND prodsourcelabel = 'user'
              UNION ALL
              SELECT pandaid,
                     NVL(inputfilebytes, 0) as inputfilebytes,
                     0                      as outputfilebytes
              FROM ATLAS_PANDA.JOBSDEFINED4
              WHERE pandaid in (SELECT distinct pandaid FROM statuses)
                AND prodsourcelabel = 'user'
    ),
    result as (SELECT s.pandaid,
                      s.queue,
                      s.status,
                      s.modificationtime,
                      j.inputfilebytes,
                      j.outputfilebytes,
                      s.lead_timestamp
               FROM statuses s
                        INNER JOIN jobs j ON (s.pandaid = j.pandaid)
               ),
    queued_execution_metrics as (
        SELECT pandaid,
               NVL(inputfilebytes,0) as inputfilebytes,
               NVL(outputfilebytes,0) as outputfilebytes,
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
                                       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')))) * 60 * 60 *
                            24),0)                                                                                       as executing_time,
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
                                                trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')))) * 60 *
                                     60 * 24),
                               0)                                                                                     as waiting_time,
                    CASE WHEN max(finished_modificationtime) < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                            THEN 1
                    ELSE 0
                        END as finished,
                    CASE WHEN COALESCE(max(failed_modificationtime),
                        max(closed_modificationtime),
                        max(cancelled_modificationtime)) < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                            THEN 1
                    ELSE 0
                        END as failed
        FROM result
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
        GROUP BY pandaid,queue,NVL(inputfilebytes,0),NVL(outputfilebytes,0)
    ),
    executing_jobs as (
        SELECT queue, count(pandaid) as n_executing_jobs,
            sum(inputfilebytes) as executing_input_volume,
            sum(outputfilebytes) as executing_output_volume
        FROM queued_execution_metrics
        where executing_time > 0
        group by queue
    ),
    queued_jobs as (
        SELECT queue, count(pandaid) as n_queued_jobs,
            sum(inputfilebytes) as queued_input_volume,
            sum(outputfilebytes) as queued_output_volume
        FROM queued_execution_metrics
        where waiting_time > 0
        group by queue
    ),
    finished as (
        SELECT queue,
               count(pandaid) as n_finished_jobs,
               sum(inputfilebytes) as finished_input_volume,
               sum(outputfilebytes) as finished_output_volume
        FROM queued_execution_metrics
        WHERE finished = 1
        group by queue
    ),
    failed as (
        SELECT queue,
               count(pandaid) as n_failed_jobs,
               sum(inputfilebytes) as failed_input_volume,
               sum(outputfilebytes) as failed_output_volume
        FROM queued_execution_metrics
        WHERE failed = 1
        group by queue
    ),
    completed as (
                SELECT queue,
               count(pandaid) as n_completed_jobs,
               sum(inputfilebytes) as completed_input_volume,
               sum(outputfilebytes) as completed_output_volume
        FROM queued_execution_metrics
        WHERE failed = 1 or finished = 1
        group by queue
    ),
    total as (
        SELECT queue,
               count(pandaid) as total_n_jobs
        from queued_execution_metrics
        group by queue
    ),
    x as (SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
                 t.queue,
                 nvl(sum(q.n_queued_jobs),0) as n_queued_jobs,
                 nvl(sum(e.n_executing_jobs),0) as n_executing_jobs,
                 nvl(sum(q.queued_input_volume)/1000000000000,0)                                  as queued_input_volume,
                 round(avg(qe.waiting_time))                                 as avg_waiting_time,
                 round(median(qe.waiting_time))                              as median_waiting_time,
                 nvl(sum(e.executing_input_volume)/1000000000000,0)                               as executing_input_volume,
                 round(avg(qe.executing_time))                               as avg_executing_time,
                 round(median(qe.executing_time))                            as median_executing_time,
                 nvl(sum(c.n_completed_jobs),0)                                     as n_completed_jobs,
                 nvl(sum(c.completed_input_volume)/1000000000000,0)                                 as completed_input_volume,
                 nvl(sum(c.completed_output_volume)/1000000000000,0)                                as completed_output_volume,
                 nvl(sum(fi.n_finished_jobs),0)                                       as n_finished_jobs,
                 nvl(sum(fi.finished_input_volume)/1000000000000,0)                                 as finished_input_volume,
                 nvl(sum(fa.n_failed_jobs),0)                                         as n_failed_jobs,
                 nvl(sum(fa.failed_input_volume)/1000000000000,0)                                   as failed_input_volume,
                 nvl(sum(t.total_n_jobs),0)                                           as total_n_jobs
          FROM total t
                   LEFT OUTER JOIN queued_execution_metrics qe ON (t.queue = qe.queue)
                   LEFT OUTER JOIN executing_jobs e on (t.queue = e.queue)
                   LEFT OUTER JOIN queued_jobs q on (t.queue = q.queue)
                   LEFT OUTER JOIN completed c on (t.queue = c.queue)
                   LEFT OUTER JOIN finished fi on (t.queue = fi.queue)
                   LEFT OUTER JOIN failed fa on (t.queue = fa.queue)
          GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
                   t.queue)
SELECT datetime,
       queue,
       n_queued_jobs,
       n_executing_jobs,
       queued_input_volume,
       avg_waiting_time,
       median_waiting_time,
       executing_input_volume,
       avg_executing_time,
       median_executing_time,
       n_completed_jobs,
       completed_input_volume,
       completed_output_volume,
       n_finished_jobs,
       finished_input_volume,
       n_failed_jobs,
       failed_input_volume,
       total_n_jobs,
       nvl(round(n_queued_jobs/nullif(n_completed_jobs,0),4),n_queued_jobs) as utilization,
       nvl(round(queued_input_volume/nullif(completed_input_volume,0),4),queued_input_volume) as utilization_weighted,
       nvl(round(n_executing_jobs/nullif(n_completed_jobs,0),4),n_executing_jobs) as fullness,
       nvl(round(executing_input_volume/nullif(completed_input_volume,0),4),executing_input_volume) as fullness_weighted,
       nvl(round(n_finished_jobs/nullif(n_completed_jobs,0),4),0) as efficiency,
       nvl(round(finished_input_volume/nullif(completed_input_volume,0),4),0) as efficiency_weighted
 FROM x