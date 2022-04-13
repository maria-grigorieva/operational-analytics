SELECT * FROM (
with jobs as (
SELECT t.jeditaskid,
      ja.pandaid,ja.creationtime,ja.starttime,ja.endtime,
       ROW_NUMBER() OVER (PARTITION BY t.jeditaskid ORDER BY ja.starttime) AS job_number
        FROM ATLAS_PANDA.JEDI_TASKS t
        INNER JOIN ATLAS_PANDA.JOBSARCHIVED4 ja ON (t.jeditaskid = ja.jeditaskid)
    WHERE
          t.tasktype = 'anal'
          AND t.modificationtime >= to_date('2022-03-19','YYYY-MM-DD')
         AND t.modificationtime < to_date('2022-03-24','YYYY-MM-DD')
          AND t.status in ('finished','done')
          AND ( ja.proddblock LIKE 'mc%' or ja.proddblock LIKE 'data%')
ORDER BY t.jeditaskid,ja.starttime),
     a as (
         SELECT jeditaskid,
                round((max(endtime) - min(starttime))*24*60*60,2) as all_jobs_duration,
                count(pandaid) as max_job_number
         FROM jobs
         GROUP BY jeditaskid
     )
SELECT jobs.jeditaskid,
       a.all_jobs_duration,
       a.max_job_number,
       round(((max(jobs.endtime)-min(jobs.starttime))*24*60*60/nullif(a.all_jobs_duration,0)),2) as fraction_of_jobs_time,
       round((max(jobs.endtime)-min(jobs.starttime))*24*60*60,2) as last_20percent_jobs_duration
FROM a, jobs
WHERE a.jeditaskid=jobs.jeditaskid AND
      jobs.job_number>=round(a.max_job_number)*0.8
        AND a.all_jobs_duration >= 86400
        AND a.all_jobs_duration is not Null
GROUP BY jobs.jeditaskid,a.all_jobs_duration,a.max_job_number)
WHERE last_20percent_jobs_duration is not Null