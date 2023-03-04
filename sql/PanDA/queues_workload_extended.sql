with a as (SELECT *
           FROM (SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1 / 24 as start_time,
                        trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')          as end_time,
                        pandaid,
                        status,
                        queue,
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
                 FROM (SELECT pandaid,
                              jobstatus     as status,
                              computingsite as queue,
                              modificationtime,
                              LEAD(CAST(modificationtime as date), 1) OVER (
                                  PARTITION BY pandaid
                                  ORDER BY modificationtime asc
                                  )         as lead_time
                       FROM ATLAS_PANDA.JOBS_STATUSLOG
                       WHERE modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
                         and modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 2
                         and prodsourcelabel = 'user'))
           WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),
                                                                    'HH24') - 1 / 24),
    b as (SELECT start_time,
                 end_time,
                 pandaid,
                 queue,
                 status,
                 min(modificationtime_real) as modificationtime_real,
                 MIN(modificationtime) as modificationtime,
                 MAX(lead_time)        as lead_time
          FROM (SELECT start_time,
                       end_time,
                       pandaid,
                       queue,
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
                           END as status,
                    modificationtime_real,
                       modificationtime,
                       lead_time
                FROM a
                WHERE status in (
                                 'pending',
                                 'defined',
                                 'assigned',
                                 'activated',
                                 'throttled',
                                 'sent',
                                 'starting',
                                 'running', 'holding', 'merging', 'transferring'
                    ))
                GROUP BY start_time,
                         end_time,
                         pandaid,
                         queue,
                         status
          ),
    c as (SELECT pandaid,
                CASE WHEN status = 'finished' THEN 'finished'
                    WHEN status in ('failed','closed','cancelled') THEN 'failed'
                    ELSE 'not_completed' END as final_status
              FROM a
              ),
    d as (SELECT b.start_time,
                 b.end_time,
                 b.pandaid,
                 b.queue,
                 b.status,
                 c.final_status,
                 round((b.lead_time - b.modificationtime) * 24 * 60 * 60) as duration
          FROM b
                   FULL OUTER JOIN c
                                   ON (b.pandaid = c.pandaid)
          ),
    e as (
    SELECT pandaid,
        gshare,
        produsername,
        transformation,
        resource_type,
        max(inputfilebytes) as inputfilebytes
    from (
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSARCHIVED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
               AND modificationtime >= (SELECT min(modificationtime_real) from b)
                and prodsourcelabel = 'user'
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                AND modificationtime >= (SELECT min(modificationtime_real) from b)
                and prodsourcelabel = 'user'
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSWAITING4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                NVL(inputfilebytes, 0) as inputfilebytes
            FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
        )
    group by pandaid,
        gshare,
        produsername,
        transformation,
        resource_type
),
f as (SELECT start_time,
             end_time,
             queue,
             gshare,
             produsername,
             transformation,
             resource_type,
             sum(running_jobs)          as running_jobs,
             sum(waiting_jobs)            as waiting_jobs,
             sum(finalizing_jobs)          as finalizing_jobs,
             sum(running_input_volume)  as running_input_volume,
             sum(waiting_input_volume)    as waiting_input_volume,
             sum(finalizing_input_volume)    as finalizing_input_volume,
             avg(running_duration)      as avg_running_duration,
             avg(waiting_duration)        as avg_waiting_duration,
             avg(finalizing_duration)        as avg_finalizing_duration,
             median(running_duration) as median_running_duration,
             median(waiting_duration) as median_waiting_duration,
             median(finalizing_duration) as median_finalizing_duration
      FROM (SELECT d.start_time,
                   d.end_time,
                   d.queue,
                   d.status,
                   e.gshare,
                   e.produsername,
                   e.transformation,
                   e.resource_type,
                   count(distinct d.pandaid)       as n_jobs,
                   round(avg(d.duration))          as duration,
                   sum(distinct e.inputfilebytes)  as input_volume
            FROM d,
                 e
            WHERE d.pandaid = e.pandaid
            GROUP BY d.start_time,
                     d.end_time,
                     d.queue,
                     d.status,
                     e.gshare,
                     e.produsername,
                     e.transformation,
                     e.resource_type)
          PIVOT (
              sum(n_jobs) as jobs,
              avg(duration) as duration,
              sum(input_volume) as input_volume
          FOR status
          IN ('running' AS running,
              'finalizing' AS finalizing,
              'waiting' AS waiting
              )
          )
      GROUP BY start_time,
               end_time,
               queue,
               gshare,
               produsername,
               transformation,
               resource_type),
    g as (SELECT start_time,
             end_time,
             queue,
             gshare,
             produsername,
             transformation,
             resource_type,
             sum(finished_jobs) as finished_jobs,
             sum(failed_jobs) as failed_jobs,
             sum(not_completed_jobs) as not_completed_jobs,
             sum(finished_input_volume) as finished_input_volume,
             sum(failed_input_volume) as failed_input_volume,
             sum(not_completed_input_volume) as not_completed_input_volume
      FROM (SELECT d.start_time,
                   d.end_time,
                   d.queue,
                   d.final_status,
                   e.gshare,
                   e.produsername,
                   e.transformation,
                   e.resource_type,
                   count(distinct d.pandaid)       as n_jobs,
                   round(avg(d.duration))          as duration,
                   sum(distinct e.inputfilebytes)  as input_volume
            FROM d,
                 e
            WHERE d.pandaid = e.pandaid
            GROUP BY d.start_time,
                     d.end_time,
                     d.queue,
                     d.final_status,
                     e.gshare,
                     e.produsername,
                     e.transformation,
                     e.resource_type)
          PIVOT (
              sum(n_jobs) as jobs,
              sum(input_volume) as input_volume
          FOR final_status
          IN ('finished' AS finished,
              'failed' AS failed,
              'not_completed' as not_completed
              )
          )
      GROUP BY start_time,
               end_time,
               queue,
               gshare,
               produsername,
               transformation,
               resource_type)
SELECT f.start_time,
               f.end_time,
               f.queue,
               f.gshare,
               f.produsername,
               f.transformation,
               f.resource_type,
               NVL(f.running_jobs,0) as running_jobs,
             NVL(f.waiting_jobs,0) as waiting_jobs,
             NVL(f.finalizing_jobs,0) as finalizing_jobs,
             NVL(f.running_input_volume,0) as running_input_volume,
             NVL(f.waiting_input_volume,0) as waiting_input_volume,
             NVL(f.finalizing_input_volume,0) as finalizing_input_volume,
             NVL(f.avg_running_duration,0) as avg_running_duration,
             NVL(f.avg_waiting_duration,0) as avg_waiting_duration,
             NVL(f.avg_finalizing_duration,0) as avg_finalizing_duration,
             NVL(f.median_running_duration,0) as median_running_duration,
             NVL(f.median_waiting_duration,0) as median_waiting_duration,
             NVL(f.median_finalizing_duration,0) as median_finalizing_duration,
             NVL(g.finished_jobs,0) as finished_jobs,
             NVL(g.failed_jobs,0) as failed_jobs,
             NVL(g.not_completed_jobs,0) as not_completed_jobs,
             NVL(g.finished_input_volume,0) as finished_input_volume,
             NVL(g.failed_input_volume,0) as failed_input_volume,
             NVL(g.not_completed_input_volume,0) as not_completed_input_volume
    FROM
f, g WHERE (f.start_time = g.start_time and
         f.end_time = g.end_time and
         f.queue = g.queue and
         f.gshare = g.gshare and
         f.produsername = g.produsername and
         f.transformation = g.transformation and
         f.resource_type = g.resource_type)