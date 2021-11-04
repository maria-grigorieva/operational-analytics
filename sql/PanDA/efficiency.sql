SELECT TRUNC(sysdate) as datetime,
       computingsite as queue,
    finished,
    failed,
    NVL(ROUND(finished / NULLIF((finished + failed),0), 4), 0) as queue_efficiency
FROM
(SELECT * FROM
    (SELECT computingsite,jobstatus,count( *) as n_jobs
    FROM ATLAS_PANDA.JOBSARCHIVED4
    WHERE modificationtime >= sysdate - 1
    AND LOWER(computingsite) NOT LIKE '%test%'
    GROUP BY computingsite, jobstatus)
    PIVOT
        (
        SUM(n_jobs)
        for jobstatus in ('closed' as closed,
        'cancelled' as cancelled,
        'failed' as failed,
        'finished' as finished
                          ))
    ORDER BY computingsite)
    ORDER BY queue_efficiency DESC