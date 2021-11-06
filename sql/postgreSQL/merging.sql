with a as (
    select src, dest, closeness
    from distances
    where datetime = (
            select max(datetime)
            from distances
        )
    and src in (
        select distinct site from datasets
        where datasetname = :ds_name
        and official = True
        and datetime = (
                select max(datetime)
                from datasets
            )
        )
    )
select *, :ds_name as datasetname,
       current_date as timestamp
from queues_metrics qm, a
where qm.site = a.dest
and qm.datetime in (
        select max(datetime)
        from queues_metrics
    )