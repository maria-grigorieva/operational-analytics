SELECT
    trunc(to_date(:from_date , 'YYYY-MM-DD HH24:MI:SS'),'DD') as datetime,
       t.creationdate,
       t.gshare,
       t.username,
        d.datasetname,
--        NVL(SUBSTR(d.datasetname, 0, INSTR(d.datasetname, '_tid')-1), d.datasetname) as datasetname,
       t.jeditaskid,
        regexp_substr(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
               (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[^_]+',1,1) as input_format_short,
    regexp_substr(regexp_replace(SUBSTR(d.datasetname, REGEXP_INSTR(d.datasetname, '\.', 1, 4) + 1,
    (REGEXP_INSTR(d.datasetname, '\.', 1, 5) -
               REGEXP_INSTR(d.datasetname, '\.', 1, 4)-1)),'[0-9]',''),'[^_]+',1,2) as input_format_desc,
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
            Instr(d.datasetname, '_tid',-1,1)-Instr(d.datasetname, '.',-1,1)-1) as process_tags
FROM ATLAS_PANDA.JEDI_TASKS t
INNER JOIN ATLAS_PANDA.JEDI_DATASETS d ON (d.jeditaskid = t.jeditaskid)
WHERE t.tasktype='anal' and
      t.creationdate >= trunc(to_date(:from_date,'YYYY-MM-DD HH24:MI:SS'),'DD') - 1 and
      t.creationdate < trunc(to_date(:from_date , 'YYYY-MM-DD HH24:MI:SS'),'DD') AND
      d.type = 'input' and
      ((d.datasetname LIKE 'mc%.DAOD%') or (d.datasetname LIKE 'data%.DAOD%')) and
      d.masterid is null
      and t.status not in ('failed','aborted','broken','aborting','tobroken','toretry')