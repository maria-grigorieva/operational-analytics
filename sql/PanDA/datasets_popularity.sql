SELECT trunc(to_date(:from_date , 'YYYY-MM-DD HH24:MI:SS'),'DD') as datetime,
       d.datasetname,
        regexp_substr(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[^_]+',1,1) as input_format_short,
        regexp_substr(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[^_]+',1,2) as input_format_desc,
        SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1) as input_project,
               SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 3) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 4) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 3)-1)) as prod_step,
        SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 2) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 3) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 2)-1)) as process_desc,
        SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 1) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 2) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 1)-1)) as n_dataset,
       substr(d.datasetname, Instr(d.datasetname, '_tid',-1,1)+1) as tid,
        SUBSTR(d.datasetname, Instr(d.datasetname, '.',-1,1)+1,
            Instr(d.datasetname, '_tid',-1,1)-Instr(d.datasetname, '.',-1,1)-1) as process_tags,
       count(distinct t.jeditaskid) as n_tasks,
       count(distinct t.username) as n_users,
       1 as used
FROM ATLAS_PANDA.JEDI_TASKS t
INNER JOIN ATLAS_PANDA.JEDI_DATASETS d ON (d.jeditaskid = t.jeditaskid)
WHERE t.tasktype='anal' and
      t.modificationtime >= trunc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),'DD') - 7 and
      t.modificationtime < trunc(to_date(:from_date , 'YYYY-MM-DD HH24:MI:SS'),'DD') AND
      t.username not in ('artprod','gangarbt') and
      d.type = 'input' and
      ((d.datasetname LIKE 'mc%.DAOD%') or (d.datasetname LIKE 'data%.DAOD%')) and
      d.masterid is null
      and t.status in ('finished','done')
GROUP BY trunc(to_date(:from_date , 'YYYY-MM-DD'),'DD'),
                d.datasetname,
         regexp_substr(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[^_]+',1,1),
        regexp_substr(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[^_]+',1,2),
        SUBSTR(d.datasetname, 1, Instr(d.datasetname, ':', -1, 1)-1),
               SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 3) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 4) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 3)-1)),
        SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 2) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 3) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 2)-1)),
        SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 1) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 2) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 1)-1)),
       substr(d.datasetname, Instr(d.datasetname, '_tid',-1,1)+1),
        SUBSTR(d.datasetname, Instr(d.datasetname, '.',-1,1)+1,
            Instr(d.datasetname, '_tid',-1,1)-Instr(d.datasetname, '.',-1,1)-1)