SELECT datetime,
       dataset,
       computingsite,
       gshare,
       inputfiletype,
       inputfileproject,
       processingtype,
       produsername,
       round(avg(nevents)) as nevents,
       round(avg(inputfilebytes)) as inputfilebytes,
       round(avg(waiting_time)) as waiting_time,
       round(avg(cpuconsumptiontime)) as cputime,
       round(avg(running_time)) as running_time,
       count(distinct pandaid) as n_jobs
FROM (SELECT (trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD')) datetime,
             pandaid,
             proddblock                                       as        dataset,
             gshare,
             regexp_replace(inputfiletype, '[0-9]', '') as inputfiletype,
             inputfileproject,
             processingtype,
             produsername,
             computingsite,
             nevents,
             inputfilebytes,
             round((starttime - creationtime) * 24 * 60 * 60) as        waiting_time,
             cpuconsumptiontime,
             round((endtime - starttime) * 24 * 60 * 60)      as        running_time
      FROM ATLAS_PANDA.JOBSARCHIVED4
      WHERE (modificationtime >= trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD') - 1 and
             modificationtime < trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD'))
        and prodsourcelabel = 'user'
        and jobstatus in ('finished', 'failed', 'closed', 'cancelled')
        and (proddblock LIKE 'mc%' or proddblock LIKE 'data%')
      and starttime is not null
      UNION ALL
      SELECT (trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD')) datetime,
             pandaid,
             proddblock as dataset,
             gshare,
             regexp_replace(inputfiletype, '[0-9]', '') as inputfiletype,
             inputfileproject,
             processingtype,
             produsername,
             computingsite,
             nevents,
             inputfilebytes,
             round((starttime - creationtime) * 24 * 60 * 60) as        waiting_time,
             cpuconsumptiontime,
             round((endtime - starttime) * 24 * 60 * 60)      as        running_time
      FROM ATLAS_PANDAARCH.JOBSARCHIVED
      WHERE (modificationtime >= trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD') - 1 and
             modificationtime < trunc(to_date(:datetime, 'YYYY-MM-DD HH24:MI:SS'), 'DD'))
        and prodsourcelabel = 'user'
        and jobstatus in ('finished', 'failed', 'closed', 'cancelled')
        and (proddblock LIKE 'mc%' or proddblock LIKE 'data%')
      and starttime is not null)
GROUP BY
    datetime,
       dataset,
       computingsite,
       gshare,
       inputfiletype,
       inputfileproject,
       processingtype,
       produsername
