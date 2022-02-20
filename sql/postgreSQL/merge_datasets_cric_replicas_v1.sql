SELECT x.* ,
     dr.available_length,
     dr.created_at,
     dr.updated_at,
     dr.accessed_at,
     dr.length,
     dr.bytes,
     dr.state as replica_state,
     dr.replica_type,
     dr.available_bytes,
     dr.scope,
     dr.events,
     dr.project,
     dr.datatype,
     dr.run_number,
     dr.stream_name,
     dr.prod_step,
     dr.version,
     dr.campaign,
     dr.access_cnt,
     dr.data_type,
     dr.data_type_desc
       FROM (
          SELECT d.datasetname,
                 d.datetime,
                 d.jeditaskid,
                 d.queue,
                 d.username,
                 cr.site,
                 cr.cloud,
                 cr.rse,
                 cr.tier_level,
                 cr.status,
                 cr.state,
                 cr.resource_type,
                 cr.region,
                 cr.corepower,
                 cr.corecount
          FROM datasets_tasks_users d
                   JOIN cric_resources cr on (d.queue = cr.queue)
          WHERE (d.datetime >= date_trunc('day', TIMESTAMP :from_date)
              and d.datetime < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))
            and (cr.datetime >= date_trunc('day', TIMESTAMP :from_date) and
                 cr.datetime < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))
                      ) x
JOIN datasets_replicas dr ON (dr.datasetname = x.datasetname)
WHERE (dr.accessed_at >= date_trunc('day', TIMESTAMP :from_date) and
           dr.accessed_at < date_trunc('day', TIMESTAMP :from_date + INTERVAL '1day'))