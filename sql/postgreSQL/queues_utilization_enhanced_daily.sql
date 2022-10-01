with queues as (
    SELECT date_trunc('day', timestamp :from_date) as datetime,
           queue,
           cpuconsumptionunit,
           site,
           cloud,
           tier_level,
           resource_type,
           round(CAST(avg(queue_efficiency) as numeric),4)  as queue_efficiency,
           round(CAST(avg(queue_utilization) as numeric),4) as queue_utilization,
           round(CAST(avg(queue_fullness) as numeric),4)    as queue_fullness,
           coalesce(sum(running),0)           as running,
           coalesce(sum(queued),0)       as queued,
           coalesce(sum(completed),0)         as completed,
           coalesce(sum(transferring),0)      as transferring
    FROM queues_utilization
    WHERE datetime >= date_trunc('day', timestamp :from_date)
      and datetime < date_trunc('day', timestamp :from_date) + interval '1 day'
    group by date_trunc('day', timestamp :from_date),
             queue,
             cpuconsumptionunit,
             site,
             cloud,
             tier_level,
             resource_type
)
 SELECT a.datetime,
        a.queue,
           a.cpuconsumptionunit,
           a.site,
           a.cloud,
           a.tier_level,
           a.resource_type,
               a.queue_efficiency,
       a.queue_fullness,
       a.queue_utilization,
        a.running,
        a.queued,
        a.completed,
        a.transferring,
        round(CAST(avg(jt.execution_time) as numeric)) as avg_execution_time,
       round(CAST(avg(jt.waiting_time) as numeric)) as avg_waiting_time,
       round(CAST(avg(jt.merging_time) as numeric)) as avg_merging_time,
       round(CAST(avg(jt.transferring_time) as numeric)) as avg_transferring_time,
       coalesce(sum(jt.inputfilebytes),0) as total_inputfilebytes,
       coalesce(sum(jt.outputfilebytes),0) as total_outputfilebytes,
        coalesce(count( distinct jt.jeditaskid),0) as n_tasks,
        coalesce(count( distinct jt.pandaid ),0) as n_jobs
FROM queues a
    INNER JOIN jobs_timings jt on (a.queue = jt.queue
                                   and jt.execution_end_tstamp between
                                    a.datetime and (a.datetime + interval '1 day'))
group by a.datetime,
        a.queue,
           a.cpuconsumptionunit,
           a.site,
           a.cloud,
           a.tier_level,
           a.resource_type,
               a.queue_efficiency,
       a.queue_fullness,
       a.queue_utilization,
        a.running,
        a.queued,
        a.completed,
        a.transferring