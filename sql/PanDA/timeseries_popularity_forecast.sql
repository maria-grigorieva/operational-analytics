SELECT trunc(t.modificationtime,'IW') as datetime,
       t.workinggroup,
        regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1) as input_format_short,
        regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2) as input_format_desc,
        SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1) as input_project,
       count(distinct t.jeditaskid) as n_tasks,
       count(distinct t.username) as n_users,
       count(distinct d.datasetname) as n_datasets
FROM ATLAS_PANDA.JEDI_TASKS t
INNER JOIN ATLAS_PANDA.JEDI_DATASETS d ON (d.jeditaskid = t.jeditaskid)
WHERE t.tasktype='anal' and
       t.modificationtime >= trunc(to_date(:from_date,'YYYY-MM-DD'),'DD') - 7 and
      t.modificationtime < trunc(to_date(:from_date, 'YYYY-MM-DD'),'DD') and
      t.username not in ('artprod','gangarbt') and
      d.type = 'input' and
      (d.datasetname LIKE 'mc%' or d.datasetname LIKE 'data%') and
      d.masterid is null and
      t.status in ('finished','done')
GROUP BY trunc(t.modificationtime,'IW'),
         t.workinggroup,
         regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,1),
         regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2),
         SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1)