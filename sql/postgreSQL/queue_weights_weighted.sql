with b as (
        SELECT tend,
               queue,
               utilization_weighted,
               fullness_weighted,
               performance_weighted,
               avg_waiting_time,
               capacity_weighted
        FROM queues_utilization_weighted
        WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '3 hour' and tend < date_trunc('hour', TIMESTAMP :now)
    ),
    f as (SELECT queue, avg(fullness_weighted) as fullness_hist_pq FROM queues_utilization_weighted
                     WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '96 hours'
                    AND tend < date_trunc('hour', TIMESTAMP :now)
                     GROUP BY queue),
    u as (SELECT queue, avg(utilization_weighted) as utilization_hist_pq FROM queues_utilization_weighted
                    WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '96 hours'
                    AND tend < date_trunc('hour', TIMESTAMP :now)
                    GROUP BY queue),
    w as (SELECT queue, avg(avg_waiting_time) as queue_time_hist_pq FROM queues_utilization_weighted
                WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '96 hours'
                    AND tend < date_trunc('hour', TIMESTAMP :now)
                GROUP BY queue),
    c as (SELECT queue, avg(capacity_weighted) as capacity_hist_pq FROM queues_utilization_weighted
                WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '96 hours'
                    AND tend < date_trunc('hour', TIMESTAMP :now)
                GROUP BY queue),
    p as (SELECT queue, avg(performance_weighted) as performance_hist_pq FROM queues_utilization_weighted
                WHERE tend >= date_trunc('hour', TIMESTAMP :now) - INTERVAL '96 hours'
                    AND tend < date_trunc('hour', TIMESTAMP :now)
                GROUP BY queue)
SELECT b.tend,
       b.queue,
       b.fullness_weighted,
       b.capacity_weighted,
       b.performance_weighted,
       b.utilization_weighted,
       b.avg_waiting_time,
       round(f.fullness_hist_pq::numeric,4) as fullness_hist_pq,
       round(u.utilization_hist_pq::numeric,4) as utilization_hist_pq,
       round(w.queue_time_hist_pq::numeric,4) as queue_time_hist_pq,
       round(c.capacity_hist_pq::numeric,4) as capacity_hist_pq,
       round(p.performance_hist_pq::numeric,4) as performance_hist_pq,
       round((w.queue_time_hist_pq - b.avg_waiting_time)::numeric,4) as queue_time_diff,
       round((c.capacity_hist_pq - b.capacity_weighted)::numeric,4) as capacity_diff,
       round((u.utilization_hist_pq - b.utilization_weighted)::numeric,4) as utilization_diff,
       round((f.fullness_hist_pq - b.fullness_weighted)::numeric,4) as fullness_diff,
       round((p.performance_hist_pq - b.performance_weighted)::numeric,4) as performance_diff
FROM b,p,c,w,u,f
WHERE b.queue = b.queue
AND p.queue = b.queue
AND c.queue = b.queue
AND w.queue = b.queue
AND u.queue = b.queue
AND f.queue = b.queue