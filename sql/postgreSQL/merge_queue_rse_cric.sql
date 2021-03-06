SELECT x.queue,
       x.datetime,
       x.running,
       x.queued,
       x.finished,
       x.failed,
       x.cancelled,
       x.closed,
       x.transferring,
       x.completed,
       x.queue_utilization,
       x.queue_fullness,
       x.queue_efficiency,
       x.avg_queue_time,
       x.avg_running_time,
       x.max_queue_time,
       x.max_running_time,
       x.median_queue_time,
       x.median_running_time,
       x.mode_queue_time,
       x.mode_running_time,
       x.site,
       x.cloud,
       x.tier_level,
       x.transferring_limit,
       x.status,
       x.state,
       x.resource_type,
       x.nodes,
       x.corepower,
       x.corecount,
       x.region,
       x.rse,
       si.avg_tombstone,
       si.difference,
       si.free_storage,
       si.min_space,
       si.persistent,
       si.primary_diff,
       si.quota_other,
       si.storage_timestamp,
       si.temporary,
       si.total_storage,
       si.unlocked,
       si.used_dark,
       si.used_other,
       si.used_rucio
FROM (
select qs.queue,
       qs.datetime,
       qs.running,
       qs.queued,
       qs.finished,
       qs.failed,
       qs.cancelled,
       qs.closed,
       qs.transferring,
       qs.completed,
       qs.queue_utilization,
       qs.queue_fullness,
       qs.queue_efficiency,
       qs.avg_queue_time,
       qs.avg_running_time,
       qs.max_queue_time,
       qs.max_running_time,
       qs.median_queue_time,
       qs.median_running_time,
       qs.mode_queue_time,
       qs.mode_running_time,
       cr.site,
       cr.cloud,
       cr.tier_level,
       cr.transferring_limit,
       cr.status,
       cr.state,
       cr.resource_type,
       cr.nodes,
       cr.corepower,
       cr.corecount,
       cr.region,
       cr.rse
from queues_snapshots qs
INNER JOIN cric_resources cr ON (cr.queue = qs.queue)
where (qs.datetime >= date_trunc('day', TIMESTAMP :from_date) and
       qs.datetime < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))
       and
       (cr.datetime >= date_trunc('day', TIMESTAMP :from_date) and
       cr.datetime < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))
) x
INNER JOIN storage_info si ON (si.rse = x.rse)
    WHERE (si.datetime >= date_trunc('day', TIMESTAMP :from_date) and
       si.datetime < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))
