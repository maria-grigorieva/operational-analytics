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
     b as (
        SELECT start_time,
                 end_time,
                 pandaid,
                 queue,
                 prodsourcelabel,
                 status,
                 min(modificationtime_real) as modificationtime_real,
                 MIN(modificationtime) as modificationtime,
                 MAX(lead_time)        as lead_time
          FROM (
              SELECT start_time,
                       end_time,
                       pandaid,
                       queue,
                       prodsourcelabel,
                       CASE
                           WHEN status in (
                                           'pending',
                                           'defined',
                                           'assigned',
                                           'activated',
                                           'throttled',
                                           'sent',
                                           'starting'
                               ) THEN 'waiting'
                           WHEN status in ('running') THEN 'running'
                           WHEN status in ('holding', 'merging', 'transferring') THEN 'finalizing'
                           ELSE status
                           END as status,
                    modificationtime_real,
                       modificationtime,
                       lead_time
                FROM a
              )
                GROUP BY start_time,
                         end_time,
                         pandaid,
                         queue,
                         prodsourcelabel,
                         status),
    d as (
        SELECT start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status,
               avg(duration) as duration
FROM (
               SELECT start_time,
                 end_time,
                 queue,
                 prodsourcelabel,
                 pandaid,
                 status,
                CASE WHEN status not in ('finished','failed','closed','cancelled') THEN
                 round((lead_time - modificationtime) * 24 * 60 * 60)
                    ELSE 0
                    END as duration
          FROM b)
group by start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               status),
    e as (
        SELECT pandaid,
        resource_type,
        actualcorecount,
        max(inputfilebytes) as inputfilebytes
        from (
            SELECT pandaid,
                resource_type,
                   actualcorecount,
                   NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSARCHIVED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
               AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                resource_type,
                   actualcorecount,
                   NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                AND modificationtime >= (SELECT min(modificationtime_real) from a)
            UNION ALL
            SELECT pandaid,
                resource_type,
                   actualcorecount,
                   NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                resource_type,
                   actualcorecount,
                   NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSWAITING4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                resource_type,
                   actualcorecount,
                   NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
        )
    group by pandaid,
        resource_type,
             actualcorecount
),
     merged as (
         SELECT e.pandaid,
                e.resource_type,
                e.actualcorecount,
                e.inputfilebytes,
                d.start_time,
                 d.end_time,
                 d.queue,
                 d.prodsourcelabel,
                 d.status,
                d.duration
         FROM e,d where e.pandaid = d.pandaid
     ),
     running as (
         SELECT start_time,
                     end_time,
                     queue,
                     prodsourcelabel,
                     resource_type,
                     count(pandaid) as running_jobs,
                     sum(actualcorecount) as running_slots,
                    round(avg(duration))  as avg_running_duration,
                    median(duration) as median_running_duration,
                   sum(inputfilebytes) as running_input_volume
             FROM merged
             WHERE status = 'running'
             GROUP BY
                 start_time,
                 end_time,
                 queue,
                 prodsourcelabel,
                 resource_type
     ),
f as (SELECT start_time,
             end_time,
             queue,
             prodsourcelabel,
             resource_type,
            NVL(sum(waiting_jobs),0) as waiting_jobs,
               NVL(sum(finalizing_jobs),0) as finalizing_jobs,
                          NVL(sum(finished_jobs),0) as finished_jobs,
             NVL(sum(failed_jobs),0) as failed_jobs,
             NVL(sum(closed_jobs),0) as closed_jobs,
             NVL(sum(cancelled_jobs),0) as cancelled_jobs,
                         NVL(sum(waiting_input_volume),0) as waiting_input_volume,
               NVL(sum(finalizing_input_volume),0) as finalizing_input_volume,
                          NVL(sum(finished_input_volume),0) as finished_input_volume,
             NVL(sum(failed_input_volume),0) as failed_input_volume,
             NVL(sum(closed_input_volume),0) as closed_input_volume,
             NVL(sum(cancelled_input_volume),0) as cancelled_input_volume,
            NVL(sum(failed_jobs),0)+NVL(sum(closed_jobs),0)+NVL(sum(cancelled_jobs),0) as not_completed_jobs,
               round(avg(waiting_duration)) as avg_waiting_duration,
               round(avg(finalizing_duration)) as avg_finalizing_duration,
             median(waiting_duration) as median_waiting_duration,
             median(finalizing_duration) as median_finalizing_duration
      FROM (SELECT start_time,
                   end_time,
                   queue,
                   status,
                   prodsourcelabel,
                   resource_type,
                   count(distinct pandaid)       as n_jobs,
                   round(avg(duration))          as duration,
                   sum(inputfilebytes) as input_volume
      FROM merged
            GROUP BY start_time,
                     end_time,
                     queue,
                     status,
                     prodsourcelabel,
                     resource_type)
          PIVOT (
              sum(n_jobs) as jobs,
              avg(duration) as duration,
              sum(input_volume) as input_volume
          FOR status
          IN ('waiting' AS waiting,
                  'finalizing' AS finalizing,
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
               resource_type)
 SELECT f.start_time,
                 f.end_time,
                 f.queue,
                 f.prodsourcelabel,
                 f.resource_type,
                 NVL(running.running_jobs,0) as running_jobs,
                 NVL(running.running_slots,0) as running_slots,
                 NVL(running.avg_running_duration,0) as avg_running_duration,
        NVL(running.median_running_duration,0) as median_running_duration,
                NVL(f.waiting_jobs,0) as waiting_jobs,
                NVL(f.finalizing_jobs,0) as finalizing_jobs,
                NVL(f.finished_jobs,0) as finished_jobs,
        NVL(f.failed_jobs,0) as failed_jobs,
        NVL(f.closed_jobs,0) as closed_jobs,
        NVL(f.cancelled_jobs,0) as cancelled_jobs,
        NVL(f.not_completed_jobs,0) as not_completed_jobs,
                NVL(f.avg_waiting_duration,0) as avg_waiting_duration,
                NVL(f.median_waiting_duration,0) as median_waiting_duration,
                NVL(f.avg_finalizing_duration,0) as avg_finalizing_duration,
                NVL(f.median_finalizing_duration,0) as median_finalizing_duration,
                                 NVL(f.waiting_input_volume,0) as waiting_input_volume,
               NVL(f.finalizing_input_volume,0) as finalizing_input_volume,
                          NVL(f.finished_input_volume,0) as finished_input_volume,
             NVL(f.failed_input_volume,0) as failed_input_volume,
             NVL(f.closed_input_volume,0) as closed_input_volume,
             NVL(f.cancelled_input_volume,0) as cancelled_input_volume,
             NVL(running.running_input_volume,0) as running_input_volume
         FROM f
                  FULL OUTER JOIN running
                                  ON (running.start_time = f.start_time) and
     (running.end_time = f.end_time) and
                  (running.queue = f.queue) and
                  (running.prodsourcelabel = f.prodsourcelabel) and
                  (running.resource_type = f.resource_type)