with curr as (SELECT site as site,
       queue as queue,
       cloud as cloud,
       tier_level as tier_level,
       resource_type,
       max(tend) as datetime,
       round(avg(avg_waiting_time)::numeric,4) as curr_waiting_time,
       round(avg(capacity_weighted)::numeric,4) as curr_capacity,
       round(avg(utilization_weighted)::numeric,4) as curr_utilization,
       round(avg(efficiency)::numeric,4) as curr_efficiency
FROM queues_weighted_jobs
WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '4 hour' and tend < :now
AND resource_type='GRID'
GROUP BY site, queue, cloud, tier_level, resource_type
),
    utilization as (
        SELECT
        round(avg(utilization_weighted)::numeric,4) as avg_utilization,
        round(PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY utilization_weighted)::numeric,4) as low_utilization,
        round(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY utilization_weighted)::numeric,4) as median_utilization,
        round(PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY utilization_weighted)::numeric,4) as high_utilization
    FROM queues_utilization_weighted
        WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '7 day' and tend < :now
        AND resource_type='GRID'
    ),
    waiting_time as (
    SELECT
    round(avg(avg_waiting_time)::numeric,4) as avg_waiting_time,
        round(PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY avg_waiting_time)::numeric,4) as low_waiting_time,
        round(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY avg_waiting_time)::numeric,4) as median_waiting_time,
        round(PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY avg_waiting_time)::numeric,4) as high_waiting_time
    FROM queues_utilization_weighted
        WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '7 day' and tend < :now
        AND resource_type='GRID'
    ),
    capacity as (
    SELECT
        round(avg(capacity_weighted)::numeric,4) as avg_capacity,
        round(PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY capacity_weighted)::numeric,4) as low_capacity,
        round(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY capacity_weighted)::numeric,4) as median_capacity,
        round(PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY capacity_weighted)::numeric,4) as high_capacity
    FROM queues_utilization_weighted
        WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '7 day' and tend < :now
        AND resource_type='GRID'
    ),
classes as (SELECT c.datetime,
       c.site,
       c.queue,
       c.cloud,
       c.resource_type,
       c.tier_level,
       c.curr_waiting_time,
       c.curr_capacity,
       c.curr_utilization,
       wt.avg_waiting_time,
       wt.low_waiting_time,
       wt.median_waiting_time,
       wt.high_waiting_time,
       cap.avg_capacity,
       cap.low_capacity,
       cap.median_capacity,
       cap.high_capacity,
       ut.avg_utilization,
       ut.low_utilization,
       ut.median_utilization,
       ut.high_utilization,
       CASE WHEN c.curr_utilization > 0 and c.curr_utilization <= ut.low_utilization THEN 1
            WHEN c.curr_utilization > ut.low_utilization and c.curr_utilization <= ut.median_utilization THEN 0
            WHEN c.curr_utilization > ut.median_utilization THEN -1
       ELSE -1
       END as "utilization_class",
       CASE WHEN c.curr_waiting_time > 0 and c.curr_waiting_time <= wt.low_waiting_time THEN 1
            WHEN c.curr_waiting_time > wt.low_waiting_time and c.curr_waiting_time <= wt.median_waiting_time THEN 0
            WHEN c.curr_waiting_time > wt.median_waiting_time THEN -1
       ELSE -1
       END as "waiting_time_class",
       CASE WHEN c.curr_capacity >= cap.median_capacity THEN 1
            WHEN c.curr_capacity >= cap.low_capacity and c.curr_capacity < cap.median_capacity THEN 0
            WHEN c.curr_capacity < cap.low_capacity THEN -1
       ELSE -1
       END as "capacity_class"
FROM curr c, capacity cap, waiting_time wt, utilization ut)
SELECT *,
       CASE WHEN (utilization_class+waiting_time_class)>=1 THEN 1
       WHEN (utilization_class+waiting_time_class)=0 THEN 0
       WHEN (utilization_class+waiting_time_class)<0 THEN -1
       ELSE -1 END as "class"
FROM classes