with tasks as (
    SELECT distinct t.jeditaskid,t.status as task_status
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
        INNER JOIN ATLAS_PANDA.TASKS_STATUSLOG s ON (t.jeditaskid = s.jeditaskid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
          AND t.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours/24
          AND t.status in ('finished','failed','done','broken','aborted')
          AND (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
          AND d.type = 'input'
          AND d.masterid is null
    ),
a as (
    SELECT jeditaskid, min(modificationtime) as initial_time,
           max(modificationtime) as max_time
    FROM ATLAS_PANDA.TASKS_STATUSLOG
    WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
    GROUP BY jeditaskid
),
    b as (
        SELECT jeditaskid,
                attemptnr,
                status,
                NVL(LAG(CAST(modificationtime as date), 1)
                     OVER (
                         PARTITION BY jeditaskid ORDER BY modificationtime ASC),
                        (SELECT min(a.initial_time)
                             FROM a
                             WHERE a.jeditaskid = jeditaskid)
                    )
                    as start_time,
                modificationtime as end_time,
                (row_number() over (PARTITION by jeditaskid
                    ORDER BY jeditaskid) - 1) as attempt_nr
        FROM ATLAS_PANDA.TASKS_STATUSLOG
        WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
        AND status in ('done', 'finished', 'failed', 'broken','aborted')
        ORDER by jeditaskid, modificationtime
    ),
    d as (SELECT jeditaskid,
                 attemptnr,
                 status,
                 modificationtime                                                as ttimestamp,
                 LEAD(CAST(modificationtime as date), 1)
                      OVER (
                          PARTITION BY jeditaskid ORDER BY modificationtime ASC) as lead_timestamp,
                 ROUND((LEAD(CAST(modificationtime as date), 1)
                             OVER (
                                 PARTITION BY jeditaskid ORDER BY modificationtime ASC) -
                        CAST(modificationtime as date)) * 60 * 60 * 24, 3)          lead
          FROM ATLAS_PANDA.TASKS_STATUSLOG
          WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
          ORDER by jeditaskid, modificationtime
          )
SELECT d.jeditaskid,
       d.attemptnr,
       d.status,
       d.ttimestamp,
       d.lead_timestamp,
       d.lead,
       min(b.attempt_nr) as fixed_attemptnr
FROM d
LEFT OUTER JOIN b ON (d.jeditaskid = b.jeditaskid)
WHERE d.ttimestamp >= b.start_time and d.ttimestamp <=b.end_time
GROUP BY d.jeditaskid,
       d.attemptnr,
       d.status,
       d.ttimestamp,
       d.lead_timestamp,
       d.lead
ORDER BY d.jeditaskid,
         d.ttimestamp