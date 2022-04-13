 SELECT * FROM (
                   SELECT workinggroup,
                          username,
                          modificationtime,
                          jeditaskid,
                          parent_tid,
                          NVL(max(case when type = 'input' then REGEXP_SUBSTR(dataset, '[^:]+', 1, 2) end),
                              'NO_INPUT')                                                   as input,
                          NVL(max(case when type = 'output' then dataset end), 'NO_OUTPUT') as output
                   FROM (
                            select distinct t.jeditaskid,
                                            t.parent_tid,
                                            t.modificationtime,
                                            NVL(jd.datasetname, 'unknown') as dataset,
                                            jd.type,
                                            t.username,
                                            t.workinggroup
                            FROM ATLAS_PANDA.JEDI_TASKS t
                                     INNER JOIN ATLAS_PANDA.JEDI_DATASETS jd ON (t.jeditaskid = jd.jeditaskid)
                            WHERE jd.type in ('input', 'output')
                              AND t.modificationtime >= sysdate - 90
                              AND t.prodsourcelabel = 'user'
                              AND t.status = 'finished'
                        )
                   GROUP BY workinggroup, username, modificationtime, jeditaskid, parent_tid
                   ORDER BY workinggroup, username, modificationtime, jeditaskid, parent_tid
               )
WHERE input LIKE 'mc16_13TeV%.DAOD_HIGG8D1.%'