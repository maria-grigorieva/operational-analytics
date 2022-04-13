SELECT jeditaskid,
       pandaid,
       starttime,
       endtime,
       queue,
       raterbytes,
       round((starttime-task_start)*24*60*60) as delta_starttime,
       job_duration,
       job_queue_time,
       delta_job_queue_time,
       delta_job_duration,
       delta_raterbytes,
       job_number,
       task_start,
       task_end,
       round((task_end-task_start)*24*60*60) as task_duration
FROM (
         SELECT t.jeditaskid,
                ja.pandaid,
                ja.creationtime,
                ja.starttime,
                ja.endtime,
                ja.computingsite                                                      as queue,
                ja.raterbytes,
                round((ja.endtime - ja.starttime) * 24 * 60 * 60)                     as job_duration,
                round((ja.starttime - ja.creationtime) * 24 * 60 * 60)                as job_queue_time,
                (round((ja.starttime - ja.creationtime) * 24 * 60 * 60) -
                 LAG(round((ja.starttime - ja.creationtime) * 24 * 60 * 60), 1)
                     OVER (PARTITION BY ja.jeditaskid
                         ORDER BY ja.jeditaskid, ja.starttime))                       as delta_job_queue_time,
                (round((ja.endtime - ja.starttime) * 24 * 60 * 60) -
                 LAG(round((ja.endtime - ja.starttime) * 24 * 60 * 60), 1)
                     OVER (PARTITION BY ja.jeditaskid
                         ORDER BY ja.jeditaskid, ja.starttime))                       as delta_job_duration,
                (ja.raterbytes - LAG(ja.raterbytes, 1)
                                     OVER (PARTITION BY ja.jeditaskid
                                         ORDER BY ja.jeditaskid, ja.starttime))       as delta_raterbytes,
                ROW_NUMBER()
                        OVER (PARTITION BY t.jeditaskid
                            ORDER BY ja.starttime)                                    AS job_number,
                min(ja.starttime)
                    OVER (PARTITION BY t.jeditaskid
                        ORDER BY t.jeditaskid,ja.starttime)                           as task_start,
                max(ja.endtime)
                    OVER (PARTITION BY t.jeditaskid
                        ORDER BY t.jeditaskid,ja.starttime)                           as task_end
         FROM ATLAS_PANDA.JEDI_TASKS t
                  INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja ON (t.jeditaskid = ja.jeditaskid)
         WHERE t.tasktype = 'anal'
           AND t.modificationtime >= TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS'), 'DD')
           AND t.modificationtime < TRUNC(to_date(:from_date, 'YYYY-MM-DD HH:MI:SS') + 1, 'DD')
           AND t.status in ('finished', 'done')
           AND (ja.proddblock LIKE 'mc%' or ja.proddblock LIKE 'data%')
           AND ja.raterbytes is not Null
               AND ja.endtime is not Null
         ORDER BY t.jeditaskid, ja.starttime
     )
WHERE round((task_end-task_start)*24*60*60) >= 80000