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
        and replica_type = 'primary'
        and datetime = (
                select max(datetime)
                from datasets
            )
        )
    )
select distinct *, :ds_name as datasetname,
       current_date as timestamp
from filtered_metrics qm, a
where qm.site = a.dest
and qm.datetime = '2021-11-08'
-- and qm.datetime in (
--         select max(datetime)
--         from filtered_metrics
--     )