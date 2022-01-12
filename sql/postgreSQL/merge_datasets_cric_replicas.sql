select x.*,
       di.n_tasks,
       di.n_jobs,
       di.avg_queue_time,
       di.avg_running_time
       from
( select
     dr.datasetname,
     dr.rse,
     dr.rse_id,
     cr.queue,
     cr.site,
     cr.cloud,
     cr.tier_level,
     cr.state as replica_state,
     cr.resource_type,
     cr.corepower,
     cr.corecount,
     cr.region,
     dr.datetime,
     dr.available_length,
     dr.created_at,
     dr.updated_at,
     dr.accessed_at,
     dr.length,
     dr.bytes,
     dr.terabytes,
     dr.comments,
     dr.state as replica_state,
     dr.account,
     dr.activity,
     dr.copies,
     dr.did_type,
     dr.expires_at,
     dr.replica_type,
     dr.available_bytes,
     dr.available_terabytes,
     dr.scope,
     dr.events as dataset_n_events,
     dr.project,
     dr.datatype,
     dr.run_number,
     dr.stream_name,
     dr.prod_step,
     dr.version,
     dr.campaign,
     dr.phys_group,
     dr.access_cnt,
     dr.data_type,
     dr.data_type_desc
     from dataset_replicas dr
     LEFT OUTER JOIN cric_resources cr ON (dr.rse = cr.rse)
WHERE cr.datetime >= :from_date and cr.datetime < :to_date
    AND dr.datetime >= :from_date and dr.datetime < :to_date) x
LEFT OUTER JOIN datasets_info di ON (di.datasetname = x.datasetname
    and di.queue = x.queue
    and di.datetime >= :from_date and di.datetime < :to_date)