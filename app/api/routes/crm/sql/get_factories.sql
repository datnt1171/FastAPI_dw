SELECT 
    factory_code,
    factory_name,
    is_active,
    has_onsite
FROM dim_factory
WHERE is_active = :is_active
    AND has_onsite = :has_onsite
LIMIT :limit OFFSET :offset;