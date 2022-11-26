with statuses as (SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    WHERE s.modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
      AND s.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND s.prodsourcelabel = 'user'),
    jobs as (
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL((COALESCE(starttime,modificationtime) - creationtime)*24*60*60,0) as jobs_timetostart
        FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL((COALESCE(starttime,modificationtime) - creationtime)*24*60*60,0) as jobs_timetostart
        FROM ATLAS_PANDAARCH.JOBSARCHIVED
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL((COALESCE(starttime,modificationtime) - creationtime)*24*60*60,0) as jobs_timetostart
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL((COALESCE(starttime,modificationtime) - creationtime)*24*60*60,0) as jobs_timetostart
        FROM ATLAS_PANDA.JOBSWAITING4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               NVL((COALESCE(starttime,modificationtime) - creationtime)*24*60*60,0) as jobs_timetostart
        FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
    ),
    input_from_files as (
        SELECT pandaid, sum(fsize) as inputfilebytes
        FROM ATLAS_PANDA.filestable4
        WHERE pandaid in (SELECT distinct pandaid
        FROM jobs
        WHERE inputfilebytes = 0) and type = 'input'
        GROUP BY pandaid
        UNION ALL
        SELECT pandaid, sum(fsize) as inputfilebytes
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
                      j.jobs_timetostart
               FROM statuses s
                        INNER JOIN jobs j ON (s.pandaid = j.pandaid)
                        LEFT OUTER JOIN input_from_files i ON (i.pandaid = s.pandaid and j.inputfilebytes = 0))
SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
                 queue,
                 NVL(sum(pending_jobs),0)                                           as pending_jobs,
                 NVL(sum(pending_volume),0)                                         as pending_volume,
                 round(NVL(avg(pending_avg_waiting_time),0))                               as avg_pending_waiting_time,
                 round(NVL(median(pending_median_waiting_time),0))                           as median_pending_waiting_time,
                 NVL(sum(defined_jobs),0)                                           as defined_jobs,
                 NVL(sum(defined_volume),0)                                         as defined_volume,
                 round(NVL(avg(defined_avg_waiting_time),0))                                 as avg_defined_waiting_time,
                 round(NVL(median(defined_median_waiting_time),0))                           as median_defined_waiting_time,
                 NVL(sum(activated_jobs),0)                                         as activated_jobs,
                 NVL(sum(activated_volume),0)                                       as activated_volume,
                 round(NVL(avg(activated_avg_waiting_time),0))                               as avg_activated_waiting_time,
                 round(NVL(median(activated_median_waiting_time),0))                           as median_activated_waiting_time,
                 NVL(sum(sent_jobs),0)                                              as sent_jobs,
                 NVL(sum(sent_volume),0)                                            as sent_volume,
                 round(NVL(avg(sent_avg_waiting_time),0))                                    as avg_sent_waiting_time,
                 round(NVL(median(sent_median_waiting_time),0))                           as median_sent_waiting_time,
                 NVL(sum(starting_jobs),0)                                          as starting_jobs,
                 NVL(sum(starting_volume),0)                                        as starting_volume,
                 round(NVL(avg(starting_avg_waiting_time),0))                                 as avg_starting_waiting_time,
                 round(NVL(median(starting_median_waiting_time),0))                           as median_starting_waiting_time,
                 NVL(sum(running_jobs),0)                                           as running_jobs,
                 NVL(sum(running_volume),0)                                         as running_volume,
                 round(NVL(avg(running_avg_waiting_time),0))                                 as avg_running_waiting_time,
                 round(NVL(median(running_median_waiting_time),0))                           as median_running_waiting_time,
                 NVL(sum(transferring_jobs),0)                                      as transferring_jobs,
                 NVL(sum(transferring_volume),0)                                    as transferring_volume,
                 round(NVL(avg(transferring_avg_waiting_time),0))                                 as avg_transferring_waiting_time,
                 round(NVL(median(transferring_median_waiting_time),0))                           as median_transferring_waiting_time,
                 NVL(sum(merging_jobs),0)                                           as merging_jobs,
                 NVL(sum(merging_volume),0)                                         as merging_volume,
                 round(NVL(avg(merging_avg_waiting_time),0))                                 as avg_merging_waiting_time,
                 round(NVL(median(merging_median_waiting_time),0))                           as median_merging_waiting_time,
                 NVL(sum(holding_jobs),0)                                           as holding_jobs,
                 NVL(sum(holding_volume),0)                                         as holding_volume,
                 round(NVL(avg(holding_avg_waiting_time),0))                                 as avg_holding_waiting_time,
                 round(NVL(median(holding_median_waiting_time),0))                           as median_holding_waiting_time,
                 NVL(sum(finished_jobs),0)                                          as finished_jobs,
                 NVL(sum(finished_volume),0)                                        as finished_volume,
                 round(NVL(avg(finished_avg_waiting_time),0))                                 as avg_finished_waiting_time,
                 round(NVL(median(finished_median_waiting_time),0))                           as median_finished_waiting_time,
                 NVL(sum(failed_jobs),0)                                            as failed_jobs,
                 NVL(sum(failed_volume),0)                                          as failed_volume,
                 round(NVL(avg(failed_avg_waiting_time),0))                                 as avg_failed_waiting_time,
                 round(NVL(median(failed_median_waiting_time),0))                           as median_failed_waiting_time,
                 NVL(sum(closed_jobs),0)                                            as closed_jobs,
                 NVL(sum(closed_volume),0)                                          as closed_volume,
                 round(NVL(avg(closed_avg_waiting_time),0))                                 as avg_closed_waiting_time,
                 round(NVL(median(closed_median_waiting_time),0))                           as median_closed_waiting_time,
                 NVL(sum(cancelled_jobs),0) as cancelled_jobs,
                 NVL(sum(cancelled_volume),0) as cancelled_volume,
                 round(NVL(avg(cancelled_avg_waiting_time),0))                                 as avg_cancelled_waiting_time,
                 round(NVL(median(cancelled_median_waiting_time),0))                           as median_cancelled_waiting_time,
                 round((NVL(avg(pending_avg_waiting_time),0)+
                 NVL(avg(defined_avg_waiting_time),0)+
                 NVL(avg(activated_avg_waiting_time),0)+
                 NVL(avg(sent_avg_waiting_time),0)+
                 NVL(avg(starting_avg_waiting_time),0))/5) as queued_avg_waiting_time,
                 round((NVL(median(pending_median_waiting_time),0)+
                 NVL(median(defined_median_waiting_time),0)+
                 NVL(median(activated_median_waiting_time),0)+
                 NVL(median(sent_median_waiting_time),0)+
                 NVL(median(starting_median_waiting_time),0))/5) as queued_median_waiting_time,
                 NVL(sum(pending_jobs),0)+
                 NVL(sum(defined_jobs),0)+
                 NVL(sum(activated_jobs),0)+
                 NVL(sum(sent_jobs),0)+
                 NVL(sum(starting_jobs),0) as queued_jobs,
                 NVL(sum(pending_volume),0)+
                 NVL(sum(defined_volume),0)+
                 NVL(sum(activated_volume),0)+
                 NVL(sum(sent_volume),0)+
                 NVL(sum(starting_volume),0) as queued_volume,
                 round((NVL(avg(finished_avg_waiting_time),0)+
                 NVL(avg(failed_avg_waiting_time),0)+
                 NVL(avg(cancelled_avg_waiting_time),0)+
                 NVL(avg(closed_avg_waiting_time),0))/4) as completed_avg_waiting_time,
                 round((NVL(median(finished_median_waiting_time),0)+
                 NVL(median(failed_median_waiting_time),0)+
                 NVL(median(cancelled_median_waiting_time),0)+
                 NVL(median(closed_median_waiting_time),0))/4) as completed_median_waiting_time,
                 round((NVL(avg(running_avg_waiting_time),0)+
                 NVL(avg(holding_avg_waiting_time),0)+
                 NVL(avg(merging_avg_waiting_time),0)+
                 NVL(avg(transferring_avg_waiting_time),0))/4) as executing_avg_waiting_time,
                 round((NVL(median(running_median_waiting_time),0)+
                 NVL(median(holding_median_waiting_time),0)+
                 NVL(median(merging_median_waiting_time),0)+
                 NVL(median(transferring_median_waiting_time),0))/4) as executing_median_waiting_time,
                 NVL(sum(finished_jobs),0)+
                 NVL(sum(failed_jobs),0)+
                 NVL(sum(closed_jobs),0)+
                 NVL(sum(cancelled_jobs),0) as completed_jobs,
                 NVL(sum(finished_volume),0)+
                 NVL(sum(failed_volume),0)+
                 NVL(sum(closed_volume),0)+
                 NVL(sum(cancelled_volume),0) as completed_volume,
                 round((NVL(sum(pending_jobs),0)+
                 NVL(sum(defined_jobs),0)+
                 NVL(sum(activated_jobs),0)+
                 NVL(sum(sent_jobs),0)+
                 NVL(sum(starting_jobs),0))/
                 nullif((NVL(sum(finished_jobs),0)+
                 NVL(sum(failed_jobs),0)+
                 NVL(sum(closed_jobs),0)+
                 NVL(sum(cancelled_jobs),0)),0),4) as utilization,
                 round(NVL(sum(pending_jobs),0)+
                 NVL(sum(defined_jobs),0)+
                 NVL(sum(activated_jobs),0)+
                 NVL(sum(sent_jobs),0)+
                 NVL(sum(starting_jobs),0)/
                 nullif((NVL(sum(running_jobs),0)+NVL(sum(holding_jobs),0)+NVL(sum(merging_jobs),0)+NVL(sum(transferring_jobs),0)),0),4) as fullness,
                 round(NVL(sum(finished_jobs),0)/nullif((NVL(sum(finished_jobs),0)+
                 NVL(sum(failed_jobs),0)+
                 NVL(sum(closed_jobs),0)+
                 NVL(sum(cancelled_jobs),0)),0),4) as efficiency,
                 round((NVL(sum(pending_volume),0)+
                 NVL(sum(defined_volume),0)+
                 NVL(sum(activated_volume),0)+
                 NVL(sum(sent_volume),0)+
                 NVL(sum(starting_volume),0))/
                 nullif((NVL(sum(finished_volume),0)+
                 NVL(sum(failed_volume),0)+
                 NVL(sum(closed_volume),0)+
                 NVL(sum(cancelled_volume),0)),0),4) as utilization_weighted,
                 round((NVL(sum(pending_volume),0)+
                 NVL(sum(defined_volume),0)+
                 NVL(sum(activated_volume),0)+
                 NVL(sum(sent_volume),0)+
                 NVL(sum(starting_volume),0))/nullif((NVL(sum(running_volume),0)+
                                                          NVL(sum(holding_volume),0)+
                                                          NVL(sum(merging_volume),0)+
                                                          NVL(sum(transferring_volume),0)),0),4) as fullness_weighted,
                 round(NVL(sum(finished_volume),0)/nullif((NVL(sum(finished_volume),0)+
                 NVL(sum(failed_volume),0)+
                 NVL(sum(closed_volume),0)+
                 NVL(sum(cancelled_volume),0)),0),4) as efficiency_weighted
          FROM result
              PIVOT (
              count(distinct pandaid) as jobs,
                  sum(inputfilebytes) as volume,
                  avg(jobs_timetostart) as avg_waiting_time,
                  median(jobs_timetostart)  as median_waiting_time
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
                   queue