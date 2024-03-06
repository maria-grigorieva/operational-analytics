with a as (SELECT *
           FROM (
               SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24 as start_time,
                        trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')          as end_time,
                        pandaid,
                        status,
                        queue,
                        prodsourcelabel,
                        modificationtime                                                     as modificationtime_real,
                        lead_time                                                            as lead_time_real,
                        CASE
                            WHEN modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24
                                and (lead_time >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24)
                                THEN trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24
                            WHEN modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24
                                and (lead_time is null) and status not in ('finished', 'failed', 'closed', 'cancelled')
                                THEN trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24
                            ELSE modificationtime
                            END                                                              as modificationtime,
                        CASE
                            WHEN (lead_time is null and status not in ('finished', 'failed', 'closed', 'cancelled'))
                                THEN trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                            ELSE lead_time
                            END                                                                 lead_time
                 FROM (
                     SELECT pandaid,
                              jobstatus     as status,
                              computingsite as queue,
                              prodsourcelabel,
                              modificationtime,
                              LEAD(CAST(modificationtime as date), 1) OVER (
                                  PARTITION BY pandaid
                                  ORDER BY modificationtime asc
                                  )         as lead_time
                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                       WHERE modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                         and modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 4
                     )
               )
               WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'HH24') - 1 / 24
    ),
    d as (
        SELECT start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status,
               avg(duration) as duration,
               avg(total_duration) as total_duration
FROM (
               SELECT a.start_time,
                 a.end_time,
                 a.queue,
                 a.prodsourcelabel,
                 a.pandaid,
                 a.status,
                CASE WHEN a.status not in ('finished','failed','closed','cancelled') THEN
                 round((a.lead_time - a.modificationtime) * 24 * 60 * 60)
                    ELSE 0
                    END as duration,
                CASE WHEN a.status not in ('finished','failed','closed','cancelled') THEN
                 round((NVL(a.lead_time_real, a.lead_time) - a.modificationtime_real) * 24 * 60 * 60)
                    ELSE 0
                    END as total_duration
          FROM a)
group by start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status),
    e as (
        SELECT pandaid,
        gshare,
        resource_type,
        actualcorecount,
        cpuconsumptiontime
        from (
            SELECT pandaid,
                gshare,
                resource_type,
                   actualcorecount,
                   cpuconsumptiontime
            FROM ATLAS_PANDA.JOBSARCHIVED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
               AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type,
                   actualcorecount,
                   cpuconsumptiontime
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type,
                   actualcorecount,
                   cpuconsumptiontime
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type,
                   actualcorecount,
                   cpuconsumptiontime
            FROM ATLAS_PANDA.JOBSWAITING4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type,
                   actualcorecount,
                   cpuconsumptiontime
            FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
        )
    group by pandaid,
        gshare,
        resource_type,
             actualcorecount,
             cpuconsumptiontime
),
     merged as (
         SELECT e.pandaid,
                e.gshare,
                e.resource_type,
                e.actualcorecount,
                e.cpuconsumptiontime,
                d.start_time,
                 d.end_time,
                 d.queue,
                 d.prodsourcelabel,
                 d.status,
                d.duration,
                d.total_duration
         FROM e,d where e.pandaid = d.pandaid
     ),
     running as (
         SELECT start_time,
                     end_time,
                     queue,
                     prodsourcelabel,
                     gshare,
                     resource_type,
                     count(pandaid) as running_jobs,
                     sum(actualcorecount) as running_slots,
                    round(avg(duration))  as avg_running_duration,
                    round(avg(total_duration)) as avg_running_total_duration,
                     round(avg(cpuconsumptiontime)) as cpuconsumptiontime
             FROM merged
             WHERE status = 'running'
             GROUP BY
                 start_time,
                 end_time,
                 queue,
                 prodsourcelabel,
                 gshare,
                 resource_type
     ),
f as (SELECT start_time,
             end_time,
             queue,
             prodsourcelabel,
             gshare,
             resource_type,
             NVL(sum(pending_jobs),0) as pending_jobs,
             NVL(sum(defined_jobs),0) as defined_jobs,
             NVL(sum(assigned_jobs),0) as assigned_jobs,
             NVL(sum(activated_jobs),0) as activated_jobs,
             NVL(sum(sent_jobs),0) as sent_jobs,
             NVL(sum(starting_jobs),0) as starting_jobs,
             NVL(sum(transferring_jobs),0) as transferring_jobs,
             NVL(sum(merging_jobs),0) as merging_jobs,
             NVL(sum(holding_jobs),0) as holding_jobs,
             NVL(sum(throttled_jobs),0) as throttled_jobs,
             NVL(sum(finished_jobs),0) as finished_jobs,
             NVL(sum(failed_jobs),0) as failed_jobs,
             NVL(sum(closed_jobs),0) as closed_jobs,
             NVL(sum(cancelled_jobs),0) as cancelled_jobs,
             NVL(sum(failed_jobs),0)+NVL(sum(closed_jobs),0)+NVL(sum(cancelled_jobs),0) as not_completed_jobs,
             round(avg(pending_duration)) as avg_pending_duration,
             round(avg(defined_duration)) as avg_defined_duration,
             round(avg(assigned_duration)) as avg_assigned_duration,
             round(avg(activated_duration)) as avg_activated_duration,
             round(avg(sent_duration)) as avg_sent_duration,
             round(avg(starting_duration)) as avg_starting_duration,
             round(avg(transferring_duration)) as avg_transferring_duration,
             round(avg(merging_duration)) as avg_merging_duration,
             round(avg(holding_duration)) as avg_holding_duration,
             round(avg(throttled_duration)) as avg_throttled_duration,
             round(avg(pending_total_duration)) as avg_pending_total_duration,
             round(avg(defined_total_duration)) as avg_defined_total_duration,
             round(avg(assigned_total_duration)) as avg_assigned_total_duration,
             round(avg(activated_total_duration)) as avg_activated_total_duration,
             round(avg(sent_total_duration)) as avg_sent_total_duration,
             round(avg(starting_total_duration)) as avg_starting_total_duration,
             round(avg(transferring_total_duration)) as avg_transferring_total_duration,
             round(avg(merging_total_duration)) as avg_merging_total_duration,
             round(avg(holding_total_duration)) as avg_holding_total_duration,
             round(avg(throttled_total_duration)) as avg_throttled_total_duration
      FROM (SELECT start_time,
                   end_time,
                   queue,
                   status,
                   prodsourcelabel,
                   gshare,
                   resource_type,
                   count(distinct pandaid)       as n_jobs,
                   round(avg(duration))          as duration,
                   round(avg(total_duration)) as total_duration
      FROM merged
            GROUP BY start_time,
                     end_time,
                     queue,
                     status,
                     prodsourcelabel,
                     gshare,
                     resource_type)
          PIVOT (
              sum(n_jobs) as jobs,
              avg(duration) as duration,
              avg(total_duration) as total_duration
          FOR status
          IN ('pending' AS pending,
                  'defined' AS defined,
                  'assigned' AS assigned,
                  'activated' AS activated,
                  'sent' AS sent,
                  'starting' AS starting,
                  'transferring' AS transferring,
                  'merging' AS merging,
                  'holding' AS holding,
                  'throttled' as throttled,
                  'finished' as finished,
                  'failed' as failed,
                  'closed' as closed,
                  'cancelled' as cancelled
                  )
          )
      GROUP BY start_time,
               end_time,
               queue,
               prodsourcelabel,
               gshare,
               resource_type)
 SELECT f.start_time,
                 f.end_time,
                 f.queue,
                 f.prodsourcelabel,
                 f.gshare,
                 f.resource_type,
                 NVL(running.running_jobs,0) as running_jobs,
                 NVL(running.running_slots,0) as running_slots,
                 NVL(running.avg_running_duration,0) as avg_running_duration,
                 NVL(running.avg_running_total_duration,0) as avg_running_total_duration,
                 NVL(running.cpuconsumptiontime,0) as cpuconsumptiontime,
                 NVL(f.pending_jobs,0) as pending_jobs,
                 NVL(f.defined_jobs,0) as defined_jobs,
                 NVL(f.assigned_jobs,0) as assigned_jobs,
                 NVL(f.activated_jobs,0) as activated_jobs,
                 NVL(f.sent_jobs,0) as sent_jobs,
                 NVL(f.starting_jobs,0) as starting_jobs,
                 NVL(f.transferring_jobs,0) as transferring_jobs,
                 NVL(f.merging_jobs,0) as merging_jobs,
                 NVL(f.holding_jobs,0) as holding_jobs,
                 NVL(f.throttled_jobs,0) as throttled_jobs,
                 NVL(f.finished_jobs,0) as finished_jobs,
                 NVL(f.failed_jobs,0) as failed_jobs,
                 NVL(f.closed_jobs,0) as closed_jobs,
                 NVL(f.cancelled_jobs,0) as cancelled_jobs,
                 NVL(f.not_completed_jobs,0) as not_completed_jobs,
                 NVL(f.avg_pending_duration,0) as avg_pending_duration,
                 NVL(f.avg_defined_duration,0) as avg_defined_duration,
                 NVL(f.avg_assigned_duration,0) as avg_assigned_duration,
                 NVL(f.avg_activated_duration,0) as avg_activated_duration,
                 NVL(f.avg_sent_duration,0) as avg_sent_duration,
                 NVL(f.avg_starting_duration,0) as avg_starting_duration,
                 NVL(f.avg_transferring_duration,0) as avg_transferring_duration,
                 NVL(f.avg_merging_duration,0) as avg_merging_duration,
                 NVL(f.avg_holding_duration,0) as avg_holding_duration,
                 NVL(f.avg_throttled_duration,0) as avg_throttled_duration,
                 NVL(f.avg_pending_total_duration,0) as avg_pending_total_duration,
                 NVL(f.avg_defined_total_duration,0) as avg_defined_total_duration,
                 NVL(f.avg_assigned_total_duration,0) as avg_assigned_total_duration,
                 NVL(f.avg_activated_total_duration,0) as avg_activated_total_duration,
                 NVL(f.avg_sent_total_duration,0) as avg_sent_total_duration,
                 NVL(f.avg_starting_total_duration,0) as avg_starting_total_duration,
                 NVL(f.avg_transferring_total_duration,0) as avg_transferring_total_duration,
                 NVL(f.avg_merging_total_duration,0) as avg_merging_total_duration,
                 NVL(f.avg_holding_total_duration,0) as avg_holding_total_duration,
                 NVL(f.avg_throttled_total_duration,0) as avg_throttled_total_duration
         FROM f
                  FULL OUTER JOIN running
                                  ON (running.start_time = f.start_time) and
     (running.end_time = f.end_time) and
                  (running.queue = f.queue) and
                  (running.prodsourcelabel = f.prodsourcelabel) and
                  (running.resource_type = f.resource_type) and
                  (running.gshare = f.gshare)