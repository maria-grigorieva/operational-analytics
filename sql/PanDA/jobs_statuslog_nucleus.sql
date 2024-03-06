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
    and status = 'transferring'
    ),
    d as (
        SELECT start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid,
               avg(duration) as duration,
               avg(total_duration) as total_duration
FROM (
               SELECT a.start_time,
                 a.end_time,
                 a.queue,
                 a.prodsourcelabel,
                 a.pandaid,
                 round((a.lead_time - a.modificationtime) * 24 * 60 * 60) as duration,
                round((NVL(a.lead_time_real, a.lead_time) - a.modificationtime_real) * 24 * 60 * 60) as total_duration
          FROM a)
group by start_time,
               end_time,
               queue,
               prodsourcelabel,
               pandaid),
    e as (
        SELECT pandaid,
        gshare,
        NVL(nucleus, 'undefined') as nucleus,
        resource_type
        from (
            SELECT pandaid,
                gshare,
                nucleus,
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
                nucleus,
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
                nucleus,
                resource_type
            FROM ATLAS_PANDA.JOBSACTIVE4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                nucleus,
                resource_type
            FROM ATLAS_PANDA.JOBSWAITING4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
            UNION ALL
            SELECT pandaid,
                gshare,
                nucleus,
                resource_type
            FROM ATLAS_PANDA.JOBSDEFINED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
        )
    group by pandaid,
        gshare,
        nucleus,
        resource_type
),
     merged as (
         SELECT e.pandaid,
                e.gshare,
                e.resource_type,
                e.nucleus,
                d.start_time,
                 d.end_time,
                 d.queue,
                 d.prodsourcelabel,
                d.duration,
                d.total_duration
         FROM e,d where e.pandaid = d.pandaid
     )
         SELECT start_time,
                     end_time,
                     queue,
                     prodsourcelabel,
                     gshare,
                     nucleus,
                     resource_type,
                     count(pandaid) as transferring_jobs,
                     round(avg(duration))  as avg_transferring_duration,
                     round(avg(total_duration)) as avg_total_transferring_duration
             FROM merged
             GROUP BY
                 start_time,
                 end_time,
                 queue,
                 prodsourcelabel,
                 gshare,
                 nucleus,
                 resource_type