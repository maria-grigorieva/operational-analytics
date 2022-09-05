WITH b as (
        SELECT site as site,
               queue as queue,
               rse as rse,
               cloud as cloud,
               tier_level as tier_level,
               corepower,
               corecount,
               queue_efficiency,
               queue_fullness,
               (SELECT avg(queue_fullness) FROM queues_snapshots
                WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week') as fullness_hist,
               queue_utilization,
               (SELECT avg(queue_utilization) FROM queues_snapshots WHERE datetime >= date_trunc('day', TIMESTAMP :now)- INTERVAL '1 week') as utilization_hist,
               avg_queue_time,
               avg_running_time,
               transferring,
               transferring_limit,
               (queued+running+completed) as daily_jobs_number,
               difference,
               datetime
        FROM resource_snapshot
        WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 day' and datetime < :now
        AND state = 'ACTIVE' and status = 'online' and queue not LIKE '%TEST%'
    ),
     aft as (SELECT queue, avg(queue_fullness) as fullness_hist_pq FROM queues_snapshots
                     WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week'
                    AND datetime < date_trunc('day', TIMESTAMP :now)
                     GROUP BY queue),
    aut as (SELECT queue, avg(queue_utilization) as utilization_hist_pq FROM queues_snapshots
                    WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week'
                    AND datetime < date_trunc('day', TIMESTAMP :now)
                    GROUP BY queue),
    aqt as (SELECT queue, avg(avg_queue_time) as queue_time_hist_pq FROM queues_snapshots
                WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week'
                AND datetime < date_trunc('day', TIMESTAMP :now)
                GROUP BY queue),
    djn as (SELECT queue, avg(queued+running+completed) as daily_jobs_number_pq FROM queues_snapshots
                WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week'
                AND datetime < date_trunc('day', TIMESTAMP :now)
                GROUP BY queue),
    art as (SELECT queue, avg(avg_running_time) as running_time_hist_pq FROM queues_snapshots
                WHERE datetime >= date_trunc('day', TIMESTAMP :now) - INTERVAL '1 week'
                AND datetime < date_trunc('day', TIMESTAMP :now)
                GROUP BY queue
        )
SELECT b.site,
       b.queue,
       b.rse,
       b.cloud,
       b.tier_level,
       b.corepower,
       b.corecount,
       b.queue_efficiency,
       b.queue_fullness,
       round(b.fullness_hist::numeric,4) as fullness_hist,
       b.queue_utilization,
       round(b.utilization_hist::numeric,4) as utilization_hist,
       b.avg_queue_time,
       b.avg_running_time,
       b.transferring,
       b.transferring_limit,
       b.daily_jobs_number,
       b.difference,
       b.datetime,
       aft.fullness_hist_pq,
       round(aut.utilization_hist_pq::numeric,4) as utilization_hist_pq,
       round(aqt.queue_time_hist_pq::numeric,4) as queue_time_hist_pq,
       round(art.running_time_hist_pq::numeric,4) as running_time_hist_pq,
       round(djn.daily_jobs_number_pq::numeric,4) as daily_jobs_number_pq,
       round((aft.fullness_hist_pq - b.queue_fullness)::numeric,4) as fullness_diff,
       round((aut.utilization_hist_pq - b.queue_utilization)::numeric,4) as utilization_diff,
       round((aqt.queue_time_hist_pq - b.avg_queue_time)::numeric,4) as queue_time_diff,
       round((art.running_time_hist_pq - b.avg_running_time)::numeric,4) as running_time_diff,
       round((djn.daily_jobs_number_pq - b.daily_jobs_number)::numeric,4) as daily_jobs_diff
FROM b,aft,aut,aqt,djn,art
WHERE aft.queue = b.queue
AND aut.queue = b.queue
AND aqt.queue = b.queue
AND djn.queue = b.queue
AND art.queue = b.queue
-- AND b.difference > 0
-- AND (b.queue_fullness > 0 AND b.queue_fullness <= b.fullness_hist)
-- AND (b.queue_utilization > 0 AND b.queue_utilization <= b.utilization_hist)