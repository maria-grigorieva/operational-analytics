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
          AND s.status in ('ready')
          AND s.attemptnr = 0
          AND (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
          AND d.type = 'input'
          AND d.masterid is null
    ),
    rerefined_tasks as (
        SELECT t.jeditaskid, t.task_status
        FROM tasks t
        INNER JOIN ATLAS_PANDA.TASKS_STATUSLOG s ON (s.jeditaskid = t.jeditaskid)
        AND s.status = 'rerefine'
    ),
    not_rerefined_tasks as (
        SELECT * FROM tasks t
                 MINUS
        SELECT * FROM rerefined_tasks
    ),
a as (
    SELECT jeditaskid, min(modificationtime) as initial_time,
           max(modificationtime) as max_time
    FROM ATLAS_PANDA.TASKS_STATUSLOG
    WHERE jeditaskid in (SELECT distinct jeditaskid FROM rerefined_tasks)
    GROUP BY jeditaskid
),
    rerefined_attempts as (
        SELECT jeditaskid,
                attemptnr,
                status,
                NVL(LAG(CAST(modificationtime as date), 1)
                     OVER (
                         PARTITION BY jeditaskid ORDER BY modificationtime ASC) + (1/24/60/60),
                        (SELECT min(a.initial_time)
                             FROM a
                             WHERE a.jeditaskid = jeditaskid)
                    )
                    as start_time,
                modificationtime as end_time,
                (row_number() over (PARTITION by jeditaskid
                    ORDER BY jeditaskid) - 1) as attempt_nr
        FROM ATLAS_PANDA.TASKS_STATUSLOG
        WHERE jeditaskid in (SELECT distinct jeditaskid FROM rerefined_tasks)
        AND status in ('done', 'finished', 'failed', 'broken','aborted')
        ORDER by jeditaskid, modificationtime
    ),
    rerefined_all_statuses as (SELECT jeditaskid,
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
          WHERE jeditaskid in (SELECT distinct jeditaskid FROM rerefined_tasks)
          ORDER by jeditaskid, modificationtime
          ),
    not_rerefined_all_statuses as (
        SELECT jeditaskid,
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
          WHERE jeditaskid in (SELECT distinct jeditaskid FROM not_rerefined_tasks)
          ORDER by jeditaskid, modificationtime
    ),
    rerefined_fixed_all_statuses as (
    SELECT d.jeditaskid,
             b.attempt_nr as attemptnr,
             d.status,
             d.ttimestamp,
             d.lead_timestamp,
             d.lead
      FROM rerefined_all_statuses d
      LEFT OUTER JOIN rerefined_attempts b ON (d.jeditaskid = b.jeditaskid)
      WHERE d.ttimestamp >= b.start_time
        and d.ttimestamp <= b.end_time
      ORDER BY d.jeditaskid,
               d.ttimestamp
      ),
    merge as (
        SELECT * FROM rerefined_fixed_all_statuses
        UNION ALL
        SELECT * FROM not_rerefined_all_statuses
    ),
    result as (SELECT jeditaskid,
                      attemptnr                                                                  as task_attemptnr,
                      CASE
                          WHEN NVL(max(finished_last),(SYSDATE - 365)) >
                                   GREATEST(NVL(max(broken_last),(SYSDATE - 365)),
                                          NVL(max(failed_last),(SYSDATE - 365)),
                                          NVL(max(done_last),(SYSDATE - 365)),
                                          NVL(max(aborted_last),(SYSDATE - 365)))
                               THEN 'finished'
                            WHEN NVL(max(broken_last),(SYSDATE - 365)) >
                                 GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                                          NVL(max(failed_last),(SYSDATE - 365)),
                                          NVL(max(done_last),(SYSDATE - 365)),
                                          NVL(max(aborted_last),(SYSDATE - 365)))
                                 THEN 'broken'
                            WHEN NVL(max(failed_last),(SYSDATE - 365)) >
                                    GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                                          NVL(max(broken_last),(SYSDATE - 365)),
                                          NVL(max(done_last),(SYSDATE - 365)),
                                          NVL(max(aborted_last),(SYSDATE - 365)))
                                  THEN 'failed'
                            WHEN NVL(max(done_last),(SYSDATE - 365)) >
                                      GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                                          NVL(max(broken_last),(SYSDATE - 365)),
                                          NVL(max(failed_last),(SYSDATE - 365)),
                                          NVL(max(aborted_last),(SYSDATE - 365)))
                                THEN 'done'
                            WHEN NVL(max(aborted_last),(SYSDATE - 365)) >
                                      GREATEST(NVL(max(finished_last),(SYSDATE - 365)),
                                          NVL(max(broken_last),(SYSDATE - 365)),
                                          NVL(max(failed_last),(SYSDATE - 365)),
                                          NVL(max(done_last),(SYSDATE - 365)))
                          THEN 'aborted'
                     ELSE 'unknown'
                     END as attempt_status,
                      min(rerefine_first)                                                              as attempt_rerefined_tstamp,
                      min(defined_first)                                                               as attempt_defined_tstamp,
                      min(ready_first)                                                                 as attempt_ready_tstamp,
                      min(running_first)                                                               as attempt_running_tstamp,
                      min(scouting_first)                                                              as attempt_scouting_tstamp,
                      NVL(min(scouting_first), min(running_first))                                     as attempt_start_tstamp,
                      round(sum(defined_lead), 3)                                                      as attempt_defined_time,
                      round(sum(ready_lead), 3)                                                        as attempt_ready_time,
                      round(sum(running_lead), 3)                                                      as attempt_running_time,
                      round(sum(pending_lead), 3)                                                      as attempt_pending_time,
                      round(sum(throttled_lead), 3)                                                    as attempt_throttled_time,
                      round(sum(exhausted_lead), 3)                                                    as attempt_exhausted_time,
                      round(sum(rerefine_lead), 3)                                                     as attempt_rerefined_time,
                      COALESCE(round(sum(finished_lead), 3),
                               round(sum(failed_lead), 3),
                               round(sum(done_lead), 3),
                               round(sum(broken_lead), 3),
                               round(sum(aborted_lead), 3))                                            as time_before_next_attempt,
                      ROUND((CAST(min(running_first) as date) -
                             CAST(min(ready_first) as date)) * 24 * 60 * 60,
                            2)                                                                         as attempt_queue_time,
                      GREATEST(NVL(max(finished_last), (SYSDATE - 365)),
                               NVL(max(broken_last), (SYSDATE - 365)),
                               NVL(max(failed_last), (SYSDATE - 365)),
                               NVL(max(done_last), (SYSDATE - 365)),
                               NVL(max(aborted_last), (SYSDATE - 365)))                                as attempt_completed_tstamp,
                      ROUND((CAST(GREATEST(NVL(max(finished_last), (SYSDATE - 365)),
                                           NVL(max(broken_last), (SYSDATE - 365)),
                                           NVL(max(failed_last), (SYSDATE - 365)),
                                           NVL(max(done_last), (SYSDATE - 365)),
                                           NVL(max(aborted_last), (SYSDATE - 365))) as date) -
                             CAST(LEAST(NVL(min(rerefine_first), SYSDATE),
                                        NVL(min(defined_first), SYSDATE),
                                        NVL(min(ready_first), SYSDATE),
                                        NVL(min(scouting_first), SYSDATE),
                                        NVL(min(running_first), SYSDATE)) as date)) * 24 * 60 * 60,
                            2)                                                                         as attempt_total_time
               FROM merge
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
               GROUP BY jeditaskid, attemptnr
               )
SELECT r.*, t.task_status
FROM result r, tasks t
WHERE r.jeditaskid = t.jeditaskid AND r.attempt_total_time >= 0