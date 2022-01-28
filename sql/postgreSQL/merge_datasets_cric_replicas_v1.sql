select x.*,
       di.jeditaskid,
       di.username
       from
( select
     dr.datasetname,
     dr.rse,
     cr.queue,
     cr.site,
     cr.cloud,
     cr.tier_level,
     cr.state,
     cr.status,
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
     dr.comments,
     dr.state as replica_state,
     dr.account,
     dr.activity,
     dr.copies,
     dr.did_type,
     dr.expires_at,
     dr.replica_type,
     dr.available_bytes,
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
     JOIN cric_resources cr ON (dr.rse = cr.rse)
WHERE cr.datetime = date_trunc('day', TIMESTAMP :from_date)
    AND dr.datetime = date_trunc('day', TIMESTAMP :from_date)) x
    JOIN datasets_tasks_users di ON (di.datasetname = x.datasetname
    and di.queue = x.queue
    and di.datetime = date_trunc('day', TIMESTAMP :from_date))