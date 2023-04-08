with statuses as (SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime,
            NVL(LEAD(CAST(s.modificationtime as date), 1)
                OVER (
                    PARTITION BY s.pandaid ORDER BY s.modificationtime ASC),
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')) as lead_timestamp,
           ROUND((NVL(LEAD(CAST(s.modificationtime as date), 1)
                OVER (
                    PARTITION BY s.pandaid ORDER BY s.modificationtime ASC),
               trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')) -
                  CAST(s.modificationtime as date))*60*60*24, 3)       lead
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    WHERE s.modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
      AND s.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND s.prodsourcelabel = 'user'),
    jobs as (
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL(outputfilebytes, 0) as outputfilebytes
        FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL(outputfilebytes, 0) as outputfilebytes
        FROM ATLAS_PANDAARCH.JOBSARCHIVED
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND prodsourcelabel = 'user'
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               0 as outputfilebytes
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               0 as outputfilebytes
        FROM ATLAS_PANDA.JOBSWAITING4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               0 as outputfilebytes
        FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND prodsourcelabel = 'user'
    ),
    input_from_files as (
        SELECT distinct pandaid, sum(fsize) as inputfilebytes
        FROM ATLAS_PANDA.filestable4
        WHERE pandaid in (SELECT distinct pandaid
        FROM jobs
        WHERE inputfilebytes = 0) and type = 'input'
        GROUP BY pandaid
        UNION ALL
        SELECT distinct pandaid, sum(fsize) as inputfilebytes
        FROM ATLAS_PANDAARCH.filestable_arch
        WHERE pandaid in (SELECT distinct pandaid
        FROM jobs
        WHERE inputfilebytes = 0) and type = 'input'
        GROUP BY pandaid
    ),
    result as (SELECT s.pandaid,
                      s.queue,
                      s.status,
                      s.modificationtime,
                      j.proddblock as datasetname,
                      j.inputfilebytes,
                      j.outputfilebytes,
                      s.lead_timestamp,
                      s.lead
               FROM statuses s
                        INNER JOIN jobs j ON (s.pandaid = j.pandaid)
                        LEFT OUTER JOIN input_from_files i ON (i.pandaid = s.pandaid and j.inputfilebytes = 0)),
    timings as (
        SELECT queue,
               count(distinct pandaid) as total_n_jobs,
               sum(inputfilebytes) as total_input_volume,
               sum(outputfilebytes) as total_output_volume,
                   round(avg(pending_time)) as avg_pending_time,
                   round(avg(defined_time)) as avg_defined_time,
                   round(avg(assigned_time)) as avg_assigned_time,
                   round(avg(activated_time)) as avg_activated_time,
                   round(avg(sent_time)) as avg_sent_time,
                   round(avg(starting_time)) as avg_starting_time,
                   round(avg(queue_time)) as avg_queue_time,
                   round(avg(running_time)) as avg_running_time,
                   round(avg(transferring_time)) as avg_transferring_time,
                   round(avg(merging_time)) as avg_merging_time,
                   round(avg(holding_time)) as avg_holding_time,
                   round(avg(executing_time)) as avg_executing_time
             FROM (
                       SELECT pandaid,
                       queue,
                       inputfilebytes,
                       outputfilebytes,
                       NVL(sum(pending_lead),0) as pending_time,
                       NVL(sum(defined_lead),0) as defined_time,
                       NVL(sum(assigned_lead),0) as assigned_time,
                       NVL(sum(activated_lead),0) as activated_time,
                       NVL(sum(sent_lead),0) as sent_time,
                       NVL(sum(starting_lead),0) as starting_time,
                       (NVL(sum(pending_lead),0)+NVL(sum(defined_lead),0)+NVL(sum(assigned_lead),0)+
                       NVL(sum(activated_lead),0)+NVL(sum(sent_lead),0)+NVL(sum(starting_lead),0)) as queue_time,
                       NVL(sum(running_lead),0) as running_time,
                       NVL(sum(transferring_lead),0) as transferring_time,
                       NVL(sum(merging_lead),0) as merging_time,
                       NVL(sum(holding_lead),0) as holding_time,
                       (NVL(sum(running_lead),0)+NVL(sum(transferring_lead),0)+NVL(sum(merging_lead),0)+
                       NVL(sum(holding_lead),0)) as executing_time
        FROM result
        PIVOT (
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
        GROUP BY pandaid,queue,inputfilebytes,outputfilebytes
        ORDER BY modificationtime)
        GROUP BY queue
    ),
    metrics as (SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
                       queue,
                       NVL(sum(pending_jobs), 0)                                   as pending_jobs,
                       NVL(sum(pending_volume), 0)                                 as pending_volume,
                       NVL(sum(defined_jobs), 0)                                   as defined_jobs,
                       NVL(sum(defined_volume), 0)                                 as defined_volume,
                       NVL(sum(activated_jobs), 0)                                 as activated_jobs,
                       NVL(sum(activated_volume), 0)                               as activated_volume,
                       NVL(sum(assigned_jobs), 0)                                 as assigned_jobs,
                       NVL(sum(assigned_volume), 0)                               as assigned_volume,
                       NVL(sum(sent_jobs), 0)                                      as sent_jobs,
                       NVL(sum(sent_volume), 0)                                    as sent_volume,
                       NVL(sum(starting_jobs), 0)                                  as starting_jobs,
                       NVL(sum(starting_volume), 0)                                as starting_volume,
                       NVL(sum(running_jobs), 0)                                   as running_jobs,
                       NVL(sum(running_volume), 0)                                 as running_volume,
                       NVL(sum(transferring_jobs), 0)                              as transferring_jobs,
                       NVL(sum(transferring_volume), 0)                            as transferring_volume,
                       NVL(sum(merging_jobs), 0)                                   as merging_jobs,
                       NVL(sum(merging_volume), 0)                                 as merging_volume,
                       NVL(sum(holding_jobs), 0)                                   as holding_jobs,
                       NVL(sum(holding_volume), 0)                                 as holding_volume,
                       NVL(sum(finished_jobs), 0)                                  as finished_jobs,
                       NVL(sum(finished_volume), 0)                                as finished_volume,
                       NVL(sum(failed_jobs), 0)                                    as failed_jobs,
                       NVL(sum(failed_volume), 0)                                  as failed_volume,
                       NVL(sum(closed_jobs), 0)                                    as closed_jobs,
                       NVL(sum(closed_volume), 0)                                  as closed_volume,
                       NVL(sum(cancelled_jobs), 0)                                 as cancelled_jobs,
                       NVL(sum(cancelled_volume), 0)                               as cancelled_volume,
                       NVL(sum(pending_jobs), 0) +
                       NVL(sum(defined_jobs), 0) +
                       NVL(sum(activated_jobs), 0) +
                       NVL(sum(sent_jobs), 0) +
                       NVL(sum(starting_jobs), 0) +
                       NVL(sum(assigned_jobs), 0)                                  as queued_jobs,
                       NVL(sum(pending_volume), 0) +
                       NVL(sum(defined_volume), 0) +
                       NVL(sum(activated_volume), 0) +
                       NVL(sum(sent_volume), 0) +
                       NVL(sum(starting_volume), 0) +
                       NVL(sum(assigned_volume), 0)                                  as queued_volume,
                       NVL(sum(finished_jobs), 0) +
                       NVL(sum(failed_jobs), 0) +
                       NVL(sum(closed_jobs), 0) +
                       NVL(sum(cancelled_jobs), 0)                                 as completed_jobs,
                       NVL(sum(finished_volume), 0) +
                       NVL(sum(failed_volume), 0) +
                       NVL(sum(closed_volume), 0) +
                       NVL(sum(cancelled_volume), 0)                               as completed_volume,
                       NVL(round((NVL(sum(pending_jobs), 0) +
                              NVL(sum(defined_jobs), 0) +
                              NVL(sum(activated_jobs), 0) +
                              NVL(sum(sent_jobs), 0) +
                              NVL(sum(starting_jobs), 0) +
                              NVL(sum(assigned_jobs), 0)) /
                             nullif((NVL(sum(finished_jobs), 0) +
                                     NVL(sum(failed_jobs), 0) +
                                     NVL(sum(closed_jobs), 0) +
                                     NVL(sum(cancelled_jobs), 0)), 0),
                             4),
                           (NVL(sum(pending_jobs), 0) +
                              NVL(sum(defined_jobs), 0) +
                              NVL(sum(activated_jobs), 0) +
                              NVL(sum(sent_jobs), 0) +
                              NVL(sum(starting_jobs), 0) +
                              NVL(sum(assigned_jobs), 0))
                           )                   as utilization,
                       NVL(round(NVL(sum(pending_jobs), 0) +
                             NVL(sum(defined_jobs), 0) +
                             NVL(sum(activated_jobs), 0) +
                             NVL(sum(sent_jobs), 0) +
                             NVL(sum(starting_jobs), 0) +
                             NVL(sum(assigned_jobs), 0)/
                             nullif((NVL(sum(running_jobs), 0) + NVL(sum(holding_jobs), 0) + NVL(sum(merging_jobs), 0) +
                                     NVL(sum(transferring_jobs), 0)), 0),
                             4),
                           (NVL(sum(pending_jobs), 0) +
                             NVL(sum(defined_jobs), 0) +
                             NVL(sum(activated_jobs), 0) +
                             NVL(sum(sent_jobs), 0) +
                             NVL(sum(starting_jobs), 0) +
                             NVL(sum(assigned_jobs), 0))
                           )                as fullness,
                       NVL(round(NVL(sum(finished_jobs), 0) / nullif((NVL(sum(finished_jobs), 0) +
                                                                  NVL(sum(failed_jobs), 0) +
                                                                  NVL(sum(closed_jobs), 0) +
                                                                  NVL(sum(cancelled_jobs), 0)), 0),4),
                           0)                                                    as efficiency,
                       NVL(round(
                            (
                             (NVL(sum(pending_volume), 0) +
                              NVL(sum(defined_volume), 0) +
                              NVL(sum(activated_volume), 0) +
                              NVL(sum(sent_volume), 0) +
                              NVL(sum(starting_volume), 0) +
                              NVL(sum(assigned_volume), 0)
                            )/1000000000000) / nullif((NVL(sum(finished_volume), 0) +
                                             NVL(sum(failed_volume), 0) +
                                             NVL(sum(closed_volume), 0) +
                                             NVL(sum(cancelled_volume), 0))/1000000000000, 0),
                            4),
                           round(
                               (
                                   (NVL(sum(pending_volume), 0) +
                                    NVL(sum(defined_volume), 0) +
                                    NVL(sum(activated_volume), 0) +
                                    NVL(sum(sent_volume), 0) +
                                    NVL(sum(starting_volume), 0) +
                                    NVL(sum(assigned_volume), 0))/1000000000000),
                               4)
                           )                 as utilization_weighted,
                       NVL(round(
                                       (
                                               (NVL(sum(pending_volume), 0) +
                                                NVL(sum(defined_volume), 0) +
                                                NVL(sum(activated_volume), 0) +
                                                NVL(sum(sent_volume), 0) +
                                                NVL(sum(starting_volume), 0) +
                                                NVL(sum(assigned_volume), 0)
                                                   ) / 1000000000000
                                           ) / nullif((NVL(sum(running_volume), 0) +
                                                       NVL(sum(holding_volume), 0) +
                                                       NVL(sum(merging_volume), 0) +
                                                       NVL(sum(transferring_volume), 0)) / 1000000000000, 0),
                                       4),
                           round(
                                       (NVL(sum(pending_volume), 0) +
                                        NVL(sum(defined_volume), 0) +
                                        NVL(sum(activated_volume), 0) +
                                        NVL(sum(sent_volume), 0) +
                                        NVL(sum(starting_volume), 0) +
                                        NVL(sum(assigned_volume), 0)) / 1000000000000, 4)
                           ) as fullness_weighted,
                       NVL(
                           round(
                               (NVL(sum(finished_volume), 0)/1000000000000) / nullif((NVL(sum(finished_volume), 0) +
                                                                                    NVL(sum(failed_volume), 0) +
                                                                                    NVL(sum(closed_volume), 0) +
                                                                                    NVL(sum(cancelled_volume), 0))/1000000000000, 0),4),0) as efficiency_weighted
                FROM result
                    PIVOT (
                    count(distinct pandaid) as jobs,
                        sum(distinct inputfilebytes) as volume
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
                GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
                         queue)
SELECT m.*,
       t.total_output_volume,
       t.avg_pending_time,
       t.avg_defined_time,
       t.avg_assigned_time,
       t.avg_activated_time,
       t.avg_sent_time,
        t.avg_starting_time,
        t.avg_queue_time,
        t.avg_running_time,
        t.avg_transferring_time,
        t.avg_merging_time,
        t.avg_holding_time,
        t. avg_executing_time,
        t.total_n_jobs
FROM metrics m, timings t
WHERE m.queue = t.queue