with statuses as (SELECT s.pandaid,
           s.computingsite                                                as queue,
           s.jobstatus                                                    as status,
           s.modificationtime
    FROM ATLAS_PANDA.JOBS_STATUSLOG s
    WHERE s.modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24/2)
      AND s.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
    AND s.prodsourcelabel = 'user'),
    jobs as (
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               processing_type, inputfiletype, inputproject
        FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24/2)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               processing_type, inputfiletype, inputproject
        FROM ATLAS_PANDAARCH.JOBSARCHIVED
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        AND modificationtime >=
          (trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24') - 1/24/2)
        AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'), 'HH24')
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               processing_type, inputfiletype, inputproject
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               processing_type, inputfiletype, inputproject
        FROM ATLAS_PANDA.JOBSWAITING4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
        UNION ALL
        SELECT pandaid, proddblock, NVL(inputfilebytes, 0) as inputfilebytes,
               processing_type, inputfiletype, inputproject
        FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE pandaid in (SELECT distinct pandaid FROM statuses)
        AND (proddblock like 'mc%' OR proddblock like 'data%')
        AND prodsourcelabel = 'user'
    ),
    input_from_files as (
        SELECT pandaid, sum(fsize) as inputfilebytes
        FROM ATLAS_PANDA.filestable4
        WHERE pandaid in (SELECT distinct pandaid
        FROM jobs
        WHERE inputfilebytes = 0) and type = 'input'
        GROUP BY pandaid
        UNION ALL
        SELECT pandaid, sum(fsize) as inputfilebytes
        FROM ATLAS_PANDAARCH.filestable_arch
        WHERE pandaid in (SELECT distinct pandaid
        FROM jobs
        WHERE inputfilebytes = 0) and type = 'input'
        GROUP BY pandaid
    )
    SELECT s.pandaid,
            s.queue,
            s.status,
            s.modificationtime,
            j.proddblock as datasetname,
            j.inputfilebytes,
            j.processing_type, j.inputfiletype, j.inputproject
     FROM statuses s
     INNER JOIN jobs j ON (s.pandaid = j.pandaid)
    LEFT OUTER JOIN input_from_files i ON (i.pandaid = s.pandaid and j.inputfilebytes = 0)