SELECT factory_code, factory_name
FROM dim_factory
WHERE factory_code = :factory_id;