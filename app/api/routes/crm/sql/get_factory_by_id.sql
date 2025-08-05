SELECT factory_code, factory_name, is_active, has_onsite
FROM dim_factory
WHERE factory_code = :factory_id;