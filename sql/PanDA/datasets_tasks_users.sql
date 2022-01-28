SELECT datasetname,
       trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') as datetime,
       jeditaskid,
       computingsite as queue,
       inputfileproject as project,
       inputfiletype as data_type,
       produsername as username
FROM (
        SELECT jeditaskid,
               computingsite,
               proddblock as datasetname,
               inputfileproject,
               inputfiletype,
               produsername,
               produserid
        FROM ATLAS_PANDA.JOBSACTIVE4
        WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          and prodsourcelabel = 'user'
        UNION
        SELECT jeditaskid,
               computingsite,
               proddblock as datasetname,
               inputfileproject,
               inputfiletype,
               produsername,
               produserid
        FROM ATLAS_PANDA.JOBSWAITING4
        WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          and prodsourcelabel = 'user'
        UNION
        SELECT jeditaskid,
               computingsite,
               proddblock as datasetname,
               inputfileproject,
               inputfiletype,
               produsername,
               produserid
        FROM ATLAS_PANDA.JOBSARCHIVED4
        WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          and prodsourcelabel = 'user'
        UNION
        SELECT jeditaskid,
               computingsite,
               proddblock as datasetname,
               inputfileproject,
               inputfiletype,
               produsername,
               produserid
        FROM ATLAS_PANDA.JOBSDEFINED4
        WHERE modificationtime >= trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')
          AND modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD')+1
          and prodsourcelabel = 'user'
    )
WHERE (datasetname LIKE 'mc%.DAOD%' or datasetname LIKE 'data%.DAOD%')
 AND (datasetname NOT LIKE '%debug%'
   OR datasetname NOT LIKE '%scout%'
   OR datasetname NOT LIKE '%hlt%'
   OR datasetname NOT LIKE '%calibration%')
GROUP BY datasetname, trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD'),
         jeditaskid, computingsite, inputfileproject,
        inputfiletype, produsername