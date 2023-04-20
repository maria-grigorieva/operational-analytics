SELECT trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') as datetime,
        t.gshare,
        t.username,
        regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1) as input_format_short,
        regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2) as input_format_desc,
        SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1) as input_project,
       count(distinct t.jeditaskid) as n_tasks,
       count(distinct d.datasetname) as n_datasets
FROM ATLAS_PANDA.JEDI_TASKS t
INNER JOIN ATLAS_PANDA.JEDI_DATASETS d ON (d.jeditaskid = t.jeditaskid)
WHERE t.tasktype='anal' and
       t.modificationtime >= trunc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),'DD') - 7 and
      t.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD') and
      t.username not in ('artprod','gangarbt') and
      d.type = 'input' and
      (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%') and
      d.masterid is null and t.status not in ('failed','aborted','broken','aborting','tobroken','toretry')
GROUP BY trunc(to_date(:from_date, 'YYYY-MM-DD HH24:MI:SS'),'DD'),
         t.gshare,
         t.username,
         regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1),
         regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2),
         SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1)