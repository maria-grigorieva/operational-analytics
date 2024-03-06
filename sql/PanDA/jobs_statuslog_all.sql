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
                    WHEN status in ('failed','closed','cancelled') THEN 'failed'
                    ELSE 'not_completed' END as final_status
              FROM a
        ),
    d as (
        SELECT a.start_time,
                 a.end_time,
                 a.pandaid,
                 a.queue,
                 a.prodsourcelabel,
                 a.status,
                 c.final_status,
                 round((a.lead_time - a.modificationtime) * 24 * 60 * 60) as duration
          FROM a
                   FULL OUTER JOIN c
                                   ON (a.pandaid = c.pandaid)
          ),
    e as (
        SELECT pandaid,
        gshare,
        resource_type
        from (
            SELECT pandaid,
                gshare,
                resource_type
            FROM ATLAS_PANDA.JOBSARCHIVED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
               AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type
            FROM ATLAS_PANDA.JOBSWAITING4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                resource_type
            FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
        )
    group by pandaid,
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
               NVL(sum(running_jobs),0) as running_jobs,
               NVL(sum(transferring_jobs),0) as transferring_jobs,
               NVL(sum(merging_jobs),0) as merging_jobs,
               NVL(sum(holding_jobs),0) as holding_jobs,
               round(avg(pending_duration)) as avg_pending_duration,
               round(avg(defined_duration)) as avg_defined_duration,
               round(avg(assigned_duration)) as avg_assigned_duration,
               round(avg(activated_duration)) as avg_activated_duration,
               round(avg(sent_duration)) as avg_sent_duration,
               round(avg(starting_duration)) as avg_starting_duration,
               round(avg(running_duration)) as avg_running_duration,
               round(avg(transferring_duration)) as avg_transferring_duration,
               round(avg(merging_duration)) as avg_merging_duration,
               round(avg(holding_duration)) as avg_holding_duration
      FROM (SELECT d.start_time,
                   d.end_time,
                   d.queue,
                   d.status,
                   d.prodsourcelabel,
                   e.gshare,
                   e.resource_type,
                   count(distinct d.pandaid)       as n_jobs,
                   round(avg(d.duration))          as duration
      FROM d,e
            WHERE d.pandaid = e.pandaid
            GROUP BY d.start_time,
                     d.end_time,
                     d.queue,
                     d.status,
                     d.prodsourcelabel,
                     e.gshare,
                     e.resource_type)
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
             sum(failed_jobs) as failed_jobs,
             sum(not_completed_jobs) as not_completed_jobs
      FROM (SELECT d.start_time,
                   d.end_time,
                   d.queue,
                   d.final_status,
                   d.prodsourcelabel,
                   e.gshare,
                   e.resource_type,
                   count(distinct d.pandaid)       as n_jobs,
                   round(avg(d.duration))          as duration
            FROM d,
                 e
            WHERE d.pandaid = e.pandaid
            GROUP BY d.start_time,
                     d.end_time,
                     d.queue,
                     d.final_status,
                     d.prodsourcelabel,
                     e.gshare,
                     e.resource_type)
          PIVOT (
              sum(n_jobs) as jobs
          FOR final_status
          IN ('finished' AS finished,
              'failed' AS failed,
              'not_completed' as not_completed
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
               NVL(f.pending_jobs,0) as pending_jobs,
               NVL(f.defined_jobs,0) as defined_jobs,
               NVL(f.assigned_jobs,0) as assigned_jobs,
               NVL(f.activated_jobs,0) as activated_jobs,
               NVL(f.sent_jobs,0) as sent_jobs,
               NVL(f.starting_jobs,0) as starting_jobs,
               NVL(f.running_jobs,0) as running_jobs,
               NVL(f.transferring_jobs,0) as transferring_jobs,
               NVL(f.merging_jobs,0) as merging_jobs,
               NVL(f.holding_jobs,0) as holding_jobs,
               NVL(f.avg_pending_duration,0) as avg_pending_duration,
               NVL(f.avg_defined_duration,0) as avg_defined_duration,
               NVL(f.avg_assigned_duration,0) as avg_assigned_duration,
               NVL(f.avg_activated_duration,0) as avg_activated_duration,
               NVL(f.avg_sent_duration,0) as avg_sent_duration,
               NVL(f.avg_starting_duration,0) as avg_starting_duration,
               NVL(f.avg_running_duration,0) as avg_running_duration,
               NVL(f.avg_transferring_duration,0) as avg_transferring_duration,
               NVL(f.avg_merging_duration,0) as avg_merging_duration,
               NVL(f.avg_holding_duration,0) as avg_holding_duration,
             NVL(g.finished_jobs,0) as finished_jobs,
             NVL(g.failed_jobs,0) as failed_jobs,
             NVL(g.not_completed_jobs,0) as not_completed_jobs
    FROM
f, g WHERE f.start_time = g.start_time and
         f.end_time = g.end_time and
         f.queue = g.queue and
         f.prodsourcelabel = g.prodsourcelabel and
         f.gshare = g.gshare and
         f.resource_type = g.resource_type