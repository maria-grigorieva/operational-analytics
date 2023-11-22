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
                         and modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 2
                     )
           WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'HH24') - 1 / 24
               )
    ),
    c as (
        SELECT pandaid,
                CASE WHEN status = 'finished' THEN 'finished'
                    WHEN status in ('failed','closed','cancelled') THEN 'not_completed'
                    ELSE 'in_progress' END as final_status
              FROM a
        ),
    d as (
        SELECT start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status,
               final_status,
               sum(duration) as duration
FROM (
               SELECT a.start_time,
                 a.end_time,
                 a.queue,
                 a.prodsourcelabel,
                 a.pandaid,
                 a.status,
                 c.final_status,
                 round((a.lead_time - a.modificationtime) * 24 * 60 * 60) as duration
          FROM a
                   FULL OUTER JOIN c
                                   ON (a.pandaid = c.pandaid))
group by start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status,
               final_status),
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
                 d.final_status,
                d.duration
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
             NVL(sum(cancelled_jobs),0) as cancelled_jobs,
             NVL(sum(closed_jobs),0) as closed_jobs,
             NVL(sum(failed_jobs),0) as failed_jobs,
               round(avg(pending_duration)) as avg_pending_duration,
               round(avg(defined_duration)) as avg_defined_duration,
               round(avg(assigned_duration)) as avg_assigned_duration,
               round(avg(activated_duration)) as avg_activated_duration,
               round(avg(sent_duration)) as avg_sent_duration,
               round(avg(starting_duration)) as avg_starting_duration,
               round(avg(transferring_duration)) as avg_transferring_duration,
               round(avg(merging_duration)) as avg_merging_duration,
               round(avg(holding_duration)) as avg_holding_duration,
             round(avg(throttled_duration)) as avg_throttled_duration
      FROM (SELECT start_time,
                   end_time,
                   queue,
                   status,
                   prodsourcelabel,
                   gshare,
                   resource_type,
                   count(distinct pandaid)       as n_jobs,
                   round(avg(duration))          as duration
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
              avg(duration) as duration
          FOR status
          IN ('pending' AS pending,
                  'defined' AS defined,
                  'assigned' AS assigned,
                  'activated' AS activated,
                  'sent' AS sent,
                  'starting' AS starting,
                  'transferring' AS transferring,
                  'merging' AS merging,
                  'finished' AS finished,
                  'failed' AS failed,
                  'holding' AS holding,
                  'cancelled' as cancelled,
                  'closed' as closed,
                  'throttled' as throttled
                  )
          )
      GROUP BY start_time,
               end_time,
               queue,
               prodsourcelabel,
               gshare,
               resource_type),
    g as (SELECT start_time,
             end_time,
             queue,
             prodsourcelabel,
             gshare,
             resource_type,
             sum(finished_jobs) as finished_jobs,
             sum(not_completed_jobs) as not_completed_jobs,
             sum(in_progress_jobs) as in_progress_jobs
      FROM (SELECT start_time,
                   end_time,
                   queue,
                   final_status,
                   prodsourcelabel,
                   gshare,
                   resource_type,
                   count(distinct pandaid) as n_jobs
            FROM merged
            GROUP BY start_time,
                     end_time,
                     queue,
                     final_status,
                     prodsourcelabel,
                     gshare,
                     resource_type)
          PIVOT (
              sum(n_jobs) as jobs
          FOR final_status
          IN ('finished' AS finished,
              'not_completed' AS not_completed,
              'in_progress' as in_progress
              )
          )
      GROUP BY start_time,
               end_time,
               queue,
               prodsourcelabel,
               gshare,
               resource_type),
     h as (
 SELECT f.start_time,
                 f.end_time,
                 f.queue,
                 f.prodsourcelabel,
                 f.gshare,
                 f.resource_type,
                 running.running_jobs,
                 running.running_slots,
                 running.avg_running_duration,
                running.cpuconsumptiontime,
                f.pending_jobs,
                f.defined_jobs,
                f.assigned_jobs,
                f.activated_jobs,
                f.sent_jobs,
                f.starting_jobs,
                f.transferring_jobs,
                f.merging_jobs,
                f.holding_jobs,
                f.throttled_jobs,
                f.cancelled_jobs,
                f.closed_jobs,
                f.failed_jobs,
                f.avg_pending_duration,
                f.avg_defined_duration,
                f.avg_assigned_duration,
                f.avg_activated_duration,
                f.avg_sent_duration,
                f.avg_starting_duration,
                f.avg_transferring_duration,
                f.avg_merging_duration,
                f.avg_holding_duration,
                f.avg_throttled_duration
        FROM running, f
            WHERE running.start_time = f.start_time and
                  running.end_time = f.end_time and
                  running.queue = f.queue and
                  running.prodsourcelabel = f.prodsourcelabel and
                  running.resource_type = f.resource_type and
                  running.gshare = f.gshare
     )
SELECT h.start_time,
               h.end_time,
               h.queue,
               h.prodsourcelabel,
               h.gshare,
               h.resource_type,
               NVL(h.pending_jobs,0) as pending_jobs,
               NVL(h.defined_jobs,0) as defined_jobs,
               NVL(h.assigned_jobs,0) as assigned_jobs,
               NVL(h.activated_jobs,0) as activated_jobs,
               NVL(h.sent_jobs,0) as sent_jobs,
               NVL(h.starting_jobs,0) as starting_jobs,
               NVL(h.running_jobs,0) as running_jobs,
               NVL(h.running_slots,0) as running_slots,
               NVL(h.avg_running_duration,0)  as avg_running_duration,
                NVL(h.cpuconsumptiontime,0) as cpuconsumptiontime,
               NVL(h.transferring_jobs,0) as transferring_jobs,
               NVL(h.merging_jobs,0) as merging_jobs,
               NVL(h.holding_jobs,0) as holding_jobs,
               NVL(h.cancelled_jobs,0) as cancelled_jobs,
               NVL(h.closed_jobs,0) as closed_jobs,
                NVL(h.throttled_jobs,0) as throttled_jobs,
               NVL(h.failed_jobs,0) as failed_jobs,
               NVL(h.avg_pending_duration,0) as avg_pending_duration,
               NVL(h.avg_defined_duration,0) as avg_defined_duration,
               NVL(h.avg_assigned_duration,0) as avg_assigned_duration,
               NVL(h.avg_activated_duration,0) as avg_activated_duration,
               NVL(h.avg_sent_duration,0) as avg_sent_duration,
               NVL(h.avg_starting_duration,0) as avg_starting_duration,
               NVL(h.avg_running_duration,0) as avg_running_duration,
               NVL(h.avg_transferring_duration,0) as avg_transferring_duration,
               NVL(h.avg_merging_duration,0) as avg_merging_duration,
               NVL(h.avg_holding_duration,0) as avg_holding_duration,
             NVL(g.finished_jobs,0) as finished_jobs,
             NVL(g.not_completed_jobs,0) as not_completed_jobs,
             NVL(g.in_progress_jobs,0) as in_progress_jobs
    FROM
h, g WHERE h.start_time = g.start_time and
         h.end_time = g.end_time and
         h.queue = g.queue and
         h.prodsourcelabel = g.prodsourcelabel and
         h.gshare = g.gshare and
         h.resource_type = g.resource_type