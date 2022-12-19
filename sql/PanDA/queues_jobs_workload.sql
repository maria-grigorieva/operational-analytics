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
    merge as (
            SELECT pandaid,
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
                    FROM prev
                   )
              ),
    jobs as (
        SELECT pandaid,
               queue,
               gshare,
               produsername,
               transformation,
               resource_type,
                max(inputfilebytes) as inputfilebytes,
                max(outputfilebytes) as outputfilebytes from (
                                SELECT pandaid,
                                      computingsite           as queue,
                                      gshare,
                                      produsername,
                                      transformation,
                                        resource_type,
                                      NVL(inputfilebytes, 0)  as inputfilebytes,
                                      NVL(outputfilebytes, 0) as outputfilebytes
                               FROM ATLAS_PANDA.JOBSARCHIVED4
                               WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                               UNION ALL
                               SELECT pandaid,
                                      computingsite           as queue,
                                      gshare,
                                      produsername,
                                       transformation,
                                        resource_type,
                                      NVL(inputfilebytes, 0)  as inputfilebytes,
                                      NVL(outputfilebytes, 0) as outputfilebytes
                               FROM ATLAS_PANDAARCH.JOBSARCHIVED
                               WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                               UNION ALL
                               SELECT pandaid,
                                      computingsite           as queue,
                                      gshare,
                                      produsername,
                                        transformation,
                                        resource_type,
                                      NVL(inputfilebytes, 0)  as inputfilebytes,
                                      NVL(outputfilebytes, 0) as outputfilebytes
                               FROM ATLAS_PANDA.JOBSACTIVE4
                               WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                               UNION ALL
                               SELECT pandaid,
                                      computingsite           as queue,
                                      gshare,
                                      produsername,
                                       transformation,
                                        resource_type,
                                      NVL(inputfilebytes, 0)  as inputfilebytes,
                                      NVL(outputfilebytes, 0) as outputfilebytes
                               FROM ATLAS_PANDA.JOBSWAITING4
                               WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                               UNION ALL
                               SELECT pandaid,
                                      computingsite           as queue,
                                      gshare,
                                      produsername,
                                       transformation,
                                        resource_type,
                                      NVL(inputfilebytes, 0)  as inputfilebytes,
                                      NVL(outputfilebytes, 0) as outputfilebytes
                               FROM ATLAS_PANDA.JOBSDEFINED4
                               WHERE pandaid IN (SELECT distinct pandaid FROM merge)
                       )
                group by pandaid, queue, gshare, produsername, transformation, resource_type
    )
        SELECT
            trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') as datetime,
            pandaid,
               queue,
               gshare,
               produsername,
                transformation,
                resource_type as job_resource_type,
               common_status,
               final_status,
               MAX(inputfilebytes) as inputfilebytes,
               MAX(outputfilebytes) as outputfilebytes,
               min(modificationtime) as modificationtime,
               max(lead_timestamp) as lead_timestamp,
               round((max(lead_timestamp) - min(modificationtime))*60*60*24) as duration
        FROM (SELECT m.pandaid,
                     m.queue,
                     j.gshare,
                     j.produsername,
                     j.transformation,
                     j.resource_type,
                     j.inputfilebytes as inputfilebytes,
                     j.outputfilebytes,
                     CASE
                         WHEN m.status in
                              ('pending', 'defined', 'assigned', 'activated', 'throttled', 'sent', 'starting')
                             THEN 'queued'
                         WHEN m.status in ('running', 'holding', 'merging', 'transferring')
                             THEN 'executing'
                         END               as common_status,
                     CASE
                         WHEN m.status in ('finished', 'failed', 'closed', 'cancelled')
                             THEN m.status
                         END as final_status,
                     m.modificationtime,
                     m.lead_timestamp
              FROM merge m
              JOIN jobs j ON (j.pandaid = m.pandaid)
              )
        GROUP BY
            trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24'),
            pandaid,
               queue,
               gshare,
               produsername,
               transformation,
                resource_type,
               common_status,
               final_status
        ORDER BY modificationtime