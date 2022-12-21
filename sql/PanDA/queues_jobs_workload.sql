with a as (SELECT start_time,
                  end_time,
                  pandaid,
                  queue,
                  status,
                  CASE
                      WHEN modificationtime < trunc(
                                                      to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                                      'HH24'
                                                  ) - 1 / 24
                          THEN trunc(to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                     'HH24'
                                   ) - 1 / 24
                      ELSE modificationtime
                      END as modificationtime,
                  lead_time
           FROM (SELECT trunc(
                                to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                'HH24'
                            ) - 1 / 24 as start_time,
                        trunc(
                                to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                'HH24'
                            )          as end_time,
                        pandaid,
                        computingsite  as queue,
                        jobstatus      as status,
                        modificationtime,
                        LEAD(CAST(modificationtime as date), 1) OVER (
                            PARTITION BY pandaid
                            ORDER BY modificationtime asc
                            )          as lead_time
                 FROM ATLAS_PANDA.JOBS_STATUSLOG
                 WHERE modificationtime <
                       trunc(
                               to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                               'HH24'
                           )
                   and modificationtime >=
                       trunc(
                               to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                               'HH24'
                           ) - 7
                   and prodsourcelabel = 'user')
           WHERE lead_time >= trunc(
                                      to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                      'HH24'
                                  ) - 1 / 24
              or lead_time is null),
    b as (
    SELECT start_time,
                  end_time,
                  pandaid,
                  queue,
                  status,
                  MIN(modificationtime) as modificationtime,
                  MAX(lead_time) as lead_time
    FROM (
    SELECT start_time,
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
                                 WHEN status in ('running', 'holding', 'merging', 'transferring') THEN 'executing'
                                 END          as status,
                  modificationtime,
                  lead_time
          FROM a
          )
    GROUP BY start_time,
                  end_time,
                  pandaid,
                  queue,
                  status
          ),
    c as (
        SELECT
                  pandaid,
                CASE
                                 WHEN status in ('finished', 'failed', 'closed', 'cancelled') THEN status
                                 END          as final_status
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
                   LEFT JOIN c
                             ON (b.pandaid = c.pandaid)
          WHERE c.final_status is not Null
            and b.lead_time is not Null
          ),
    e as (
    SELECT pandaid,
        gshare,
        produsername,
        transformation,
        resource_type,
        cpuconsumptionunit,
        proddblock,
        processingtype,
        inputfiletype,
        inputfileproject,
        max(inputfilebytes) as inputfilebytes,
        max(outputfilebytes) as outputfilebytes
    from (
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject,
                NVL(inputfilebytes, 0) as inputfilebytes,
                NVL(outputfilebytes, 0) as outputfilebytes
            FROM ATLAS_PANDA.JOBSARCHIVED4
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                and modificationtime >= trunc(
                               to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                               'HH24'
                           ) - 7
                and modificationtime < trunc(
                                            to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                            'HH24'
                                        )
                and prodsourcelabel = 'user'
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject,
                NVL(inputfilebytes, 0) as inputfilebytes,
                NVL(outputfilebytes, 0) as outputfilebytes
            FROM ATLAS_PANDAARCH.JOBSARCHIVED
            WHERE pandaid IN (
                    SELECT distinct pandaid
                    FROM d
                )
                and modificationtime >= trunc(
                               to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                               'HH24'
                           ) - 7
                and modificationtime < trunc(
                                            to_date(:end_date, 'YYYY-MM-DD HH24:MI:SS'),
                                            'HH24'
                                        )
                and prodsourcelabel = 'user'
            UNION ALL
            SELECT pandaid,
                gshare,
                produsername,
                transformation,
                resource_type,
                cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject,
                NVL(inputfilebytes, 0) as inputfilebytes,
                NVL(outputfilebytes, 0) as outputfilebytes
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
                cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject,
                NVL(inputfilebytes, 0) as inputfilebytes,
                NVL(outputfilebytes, 0) as outputfilebytes
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
                cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject,
                NVL(inputfilebytes, 0) as inputfilebytes,
                NVL(outputfilebytes, 0) as outputfilebytes
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
        resource_type,
                        cpuconsumptionunit,
                proddblock,
                processingtype,
                inputfiletype,
                inputfileproject
)
SELECT d.start_time,
       d.end_time,
       d.pandaid,
       d.queue,
       d.status,
       d.final_status,
       d.duration,
       e.gshare,
       e.produsername,
       e.transformation,
       e.resource_type,
       e.cpuconsumptionunit,
       e.proddblock,
       e.processingtype,
       e.inputfiletype,
       e.inputfileproject,
       e.inputfilebytes,
       e.outputfilebytes
FROM d, e WHERE d.pandaid = e.pandaid