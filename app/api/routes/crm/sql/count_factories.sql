SELECT COUNT (*)
FROM dim_factory
WHERE is_active = :is_active
    AND has_onsite = :has_onsite