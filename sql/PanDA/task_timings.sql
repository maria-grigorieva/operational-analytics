with tasks as (
    SELECT distinct t.jeditaskid,t.status as task_status,t.username,t.gshare, t.creationdate as task_creationdate,
                    t.processingtype,
                    t.taskpriority,
                    t.currentpriority,
                    t.architecture,
                    t.transuses,
                    t.transhome,
                    t.transpath,
                    t.resource_type,
                    t.splitrule,
                    t.corecount,
                    t.basewalltime,
                    t.ramcount,
                    t.outdiskcount,
                    t.termcondition
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.TASKS_STATUSLOG s ON (t.jeditaskid = s.jeditaskid)
        INNER JOIN ATLAS_PANDA.jedi_datasets d ON (t.jeditaskid = d.jeditaskid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND t.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          AND t.status = 'done'
          AND s.status in ('ready')
          AND s.attemptnr = 0
          AND (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
          AND ( d.datasetname NOT LIKE '%debug%'
            OR d.datasetname NOT LIKE '%scout%'
            OR d.datasetname NOT LIKE '%hlt%'
            OR d.datasetname NOT LIKE '%calibration%')
          AND t.processingtype not like 'gangarobot%'
          AND t.username not in ('artprod','gangarbt')
          AND t.gShare != 'Test'
          AND d.type = 'input'
          AND d.masterid is null
    GROUP BY t.jeditaskid,t.status,t.username,t.gshare, t.creationdate,
                                 t.processingtype,
                    t.taskpriority,
                    t.currentpriority,
                    t.architecture,
                    t.transuses,
                    t.transhome,
                    t.transpath,
                    t.resource_type,
                    t.splitrule,
                    t.taskname,
                    t.corecount,
                    t.basewalltime,
                    t.ramcount,
                    t.outdiskcount,
                    t.termcondition
    ),
    numbers as (
        SELECT d.jeditaskid,
            sum(d.nfiles) as nfiles,
               sum(d.nfilestobeused) as nfilestobeused,
               sum(d.nfilesused) as nfilesused,
               sum(d.nevents) as nevents,
               sum(d.neventstobeused) as neventstobeused,
               sum(d.neventsused) as neventsused,
               sum(d.nfilesfinished) as nfilesfinished,
               sum(d.nfilesfailed) as nfilesfailed,
                MIN(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]','')) as input_format,
                MIN(regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1)) as input_format_short,
                MIN(regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
                       (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
                       REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2)) as input_format_desc,
                    MIN(SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1)) as input_project,
                    count(distinct d.datasetname) as ndatasets
        FROM ATLAS_PANDA.jedi_datasets d
        INNER JOIN tasks t ON (t.jeditaskid = d.jeditaskid)
        WHERE (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%')
        AND d.type = 'input' AND d.masterid is null
        AND d.status = 'done'
        GROUP BY d.jeditaskid
    ),
attempts as (
    SELECT * FROM ATLAS_PANDA.TASK_ATTEMPTS
    WHERE jeditaskid in (SELECT distinct jeditaskid FROM tasks)
),
    all_statuses as (SELECT jeditaskid,
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
          ),
    fixed_all_statuses as (
    SELECT d.jeditaskid,
             b.attemptnr,
             b.startstatus,
             b.endstatus,
             b.starttime,
             b.endtime,
             d.status,
             d.ttimestamp,
             d.lead_timestamp,
             d.lead
      FROM all_statuses d
      LEFT OUTER JOIN attempts b ON (d.jeditaskid = b.jeditaskid)
      WHERE d.ttimestamp >= b.starttime
        and d.ttimestamp <= b.endtime + (1/24/60/60)
      ORDER BY d.jeditaskid,
               d.ttimestamp
      ),
    result as (SELECT jeditaskid,
                      attemptnr,
                      startstatus,
                      endstatus,
                      starttime as start_time,
                      endtime as end_time,
                      round((cast(endtime as date) - cast(starttime as date))*24*60*60,2) as total_time,
                      min(rerefine_first)                                                              as rerefined_tstamp,
                      min(defined_first)                                                               as defined_tstamp,
                      min(ready_first)                                                                 as ready_tstamp,
                      min(running_first)                                                               as running_tstamp,
                      min(scouting_first)                                                              as scouting_tstamp,
                      round(sum(defined_lead), 3)                                                      as defined_time,
                      round(sum(ready_lead), 3)                                                        as ready_time,
                      round(sum(scouting_lead),3)                                                      as scouting_time,
                      round(sum(running_lead), 3)                                                      as running_time,
                      round(sum(pending_lead), 3)                                                      as pending_time,
                      round(sum(throttled_lead), 3)                                                    as throttled_time,
                      round(sum(exhausted_lead), 3)                                                    as exhausted_time,
                      COALESCE(round(sum(finished_lead), 3),
                               round(sum(failed_lead), 3),
                               round(sum(done_lead), 3),
                               round(sum(broken_lead), 3),
                               round(sum(aborted_lead), 3))                                            as time_before_next_attempt,
                      ROUND((CAST(min(running_first) as date) -
                             CAST(min(ready_first) as date)) * 24 * 60 * 60,
                            2)                                                                         as waiting_time
               FROM fixed_all_statuses
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
               GROUP BY jeditaskid, attemptnr,
                      startstatus,
                      endstatus, starttime, endtime
               )
SELECT r.*,t.username,t.task_status,t.gshare,t.task_creationdate,
                           t.processingtype,
                    t.taskpriority,
                    t.currentpriority,
                    t.architecture,
                    t.transuses,
                    t.transhome,
                    t.transpath,
                    t.resource_type,
                    t.splitrule,
                    t.corecount,
                    t.basewalltime,
                    t.ramcount,
                    t.outdiskcount,
                    t.termcondition,
       n.nfiles,
       n.nfilestobeused,
       n.nfilesused,
       n.nevents,
       n.neventstobeused,
       n.neventsused,
       n.nfilesfinished,
       n.nfilesfailed,
       n.input_format,
       n.input_project,
       n.ndatasets,
       n.input_format_short,
       n.input_format_desc
FROM result r, tasks t, numbers n
WHERE r.jeditaskid = t.jeditaskid AND n.jeditaskid = t.jeditaskid
ORDER BY t.jeditaskid, r.attemptnr