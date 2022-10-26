INSERT INTO aggregated_datasets_popularity (datasetname,
                                            input_format_desc,
                                            input_format_short,
                                            format_desc,
                                            input_project,
                                            prod_step,
                                            process_desc,
                                            n_dataset,
                                            tid,
                                            process_tags,
                                            n_tasks,
                                            start_usage,
                                            end_usage,
                                            usage_period)
VALUES(:datasetname,
       :input_format_desc,
       :input_format_short,
       :format_desc,
       :input_project,
       :prod_step,
       :process_step,
       :n_dataset,
       :tid,
       :process_tags,
       :n_tasks,
       :start_usage,
       :end_usage,
       :usage_period)
ON CONFLICT (datasetname)
DO
   UPDATE SET end_usage = EXCLUDED.end_usage,
              n_tasks = n_tasks + EXCLUDED.n_tasks,
              usage_period = DATE_PART('day', (EXCLUDED.end_usage - start_usage))