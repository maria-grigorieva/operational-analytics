-- fixed rerefined tasks statuses
with tasks as (
    SELECT t.jeditaskid,t.status as task_status
        FROM ATLAS_PANDA.JEDI_TASKS t
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
          AND t.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours/24
          AND t.status in ('finished','failed','done','broken','aborted')
            GROUP BY t.jeditaskid,t.status
    UNION
    SELECT t.jeditaskid,t.status as task_status
        FROM ATLAS_PANDA.JEDI_TASKS t
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS')
          AND t.modificationtime < to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS') + :hours/24
          AND t.status in ('finished','failed','done','broken','aborted')
            GROUP BY t.jeditaskid,t.status
),
    rerefined as (
        SELECT jeditaskid from ATLAS_PANDA.TASKS_STATUSLOG
           WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
           AND status = 'rerefine'
    ),
a as (
    SELECT jeditaskid, min(modificationtime) as initial_time
    FROM ATLAS_PANDA.TASKS_STATUSLOG
    WHERE jeditaskid in (SELECT distinct jeditaskid FROM rerefined)
    GROUP BY jeditaskid
),
    b as (
        SELECT jeditaskid,
                attemptnr,
                status,
                NVL(LAG(CAST(modificationtime as date), 1)
                     OVER (
                         PARTITION BY jeditaskid ORDER BY modificationtime ASC), (SELECT min(a.initial_time)
                             FROM a
                             WHERE a.jeditaskid = jeditaskid))
                    as start_time,
                modificationtime as end_time,
                (row_number() over (PARTITION by jeditaskid
                    ORDER BY jeditaskid) - 1) as attempt_nr
        FROM ATLAS_PANDA.TASKS_STATUSLOG
        WHERE
         jeditaskid in (SELECT distinct jeditaskid FROM rerefined)
            AND status in ('done', 'finished', 'failed', 'broken')
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
          WHERE jeditaskid in (SELECT distinct jeditaskid FROM rerefined)
          ORDER by jeditaskid, modificationtime
          )
SELECT d.jeditaskid,
       d.attemptnr,
       d.status,
       d.ttimestamp,
       d.lead_timestamp,
       d.lead,
       (SELECT min(attempt_nr) FROM b where b.jeditaskid = d.jeditaskid and (d.ttimestamp >= start_time
                                and d.ttimestamp <=end_time)) as attempt_nr
FROM d;

SELECT jeditaskid,
           attempt_nr as task_attemptnr,
           min(rerefine_first) as attempt_rerefined_first,
           min(defined_first) as attempt_defined_first,
           min(ready_first) as attempt_ready_first,
           min(running_first) as attempt_running_first,
           min(scouting_first) as attempt_scouting_first,
           NVL(min(scouting_first),min(running_first)) as attempt_start,
          GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
              NVL(max(broken_last),(SYSDATE - 365)),
              NVL(max(failed_last),(SYSDATE - 365)),
              NVL(max(done_last),(SYSDATE - 365)),
              NVL(max(aborted_last),(SYSDATE - 365))) as attempt_completed,
            CASE WHEN NVL(max(finished_last), null) is not null THEN 'finished'
                 WHEN NVL(max(failed_last), null) is not null THEN 'failed'
                 WHEN NVL(max(done_last), null) is not null THEN 'done'
                 WHEN NVL(max(broken_last), null) is not null THEN 'broken'
                 WHEN NVL(max(aborted_last), null) is not null THEN 'aborted'
            ELSE 'not completed' END as attempt_status,
          round(sum(defined_lead), 3) as attempt_defined_time,
          round(sum(ready_lead), 3) as attempt_ready_time,
          round(sum(running_lead), 3) as attempt_running_time,
          round(sum(pending_lead), 3) as attempt_pending_time,
          round(sum(throttled_lead), 3) as attempt_throttled_time,
          round(sum(exhausted_lead), 3) as attempt_exhausted_time,
          round(sum(rerefine_lead), 3) as attempt_rerefined_time,
          COALESCE(round(sum(finished_lead), 3),
                   round(sum(failed_lead), 3),
                   round(sum(done_lead), 3),
                   round(sum(broken_lead), 3),
                   round(sum(aborted_lead), 3)) as attempt_completed_state_time,
          ROUND((CAST(min(running_first) as date) - CAST(min(ready_first) as date))*24*60*60, 2) as attempt_queue_time
    FROM (
        with a as (
    SELECT jeditaskid, min(modificationtime) as initial_time
    FROM ATLAS_PANDA.TASKS_STATUSLOG
        WHERE jeditaskid = 29707180
        GROUP BY jeditaskid
),
    b as (
        SELECT jeditaskid,
                attemptnr,
                status,
                NVL(LAG(CAST(modificationtime as date), 1)
                     OVER (
                         PARTITION BY jeditaskid ORDER BY modificationtime ASC), (SELECT a.initial_time
                             FROM a
                             WHERE a.jeditaskid = jeditaskid))
                    as start_time,
                modificationtime as end_time,
                                row_number() over (PARTITION by jeditaskid
                    ORDER BY jeditaskid) as attempt_nr
        FROM ATLAS_PANDA.TASKS_STATUSLOG
        WHERE jeditaskid = 29707180
        and status in ('done', 'finished', 'failed', 'broken', 'aborted')
        ORDER by jeditaskid, modificationtime
            ),
            d as (SELECT jeditaskid,
                         attemptnr,
                         status,
                         modificationtime                                                as ttimestamp,
                         ROUND((LEAD(CAST(modificationtime as date), 1)
                                     OVER (
                                         PARTITION BY jeditaskid ORDER BY modificationtime ASC) -
                                CAST(modificationtime as date)) * 60 * 60 * 24, 3)          lead
                  FROM ATLAS_PANDA.TASKS_STATUSLOG
                  WHERE jeditaskid = 29707180
                  )
        SELECT d.jeditaskid,
               d.attemptnr,
               d.status,
               d.ttimestamp,
               d.lead,
               (SELECT min(attempt_nr) FROM b where b.jeditaskid = d.jeditaskid and (d.ttimestamp >= start_time
                                        and d.ttimestamp <=end_time)) as attempt_nr
        FROM d
        )
    PIVOT (
        min(ttimestamp) as first,
        max(ttimestamp) as last,
        sum(lead) as lead
        FOR status
        IN ('defined' AS defined,
          'ready' AS ready,
          'running' AS running,
          'pending' AS pending,
          'scouting' AS scouting,
          'throttled' AS throttled,
          'exhausted' AS exhausted,
          'finished' AS finished,
          'done' AS done,
          'failed' AS failed,
          'aborted' AS aborted,
          'broken' AS broken,
          'rerefine' as rerefine
          )
    )
    GROUP BY jeditaskid, attempt_nr;