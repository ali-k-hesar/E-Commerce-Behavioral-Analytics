SELECT 
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnotnull AS not_null,
    a.attnum AS position
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'public.aisles'::regclass
  AND a.attnum > 0 
  AND NOT a.attisdropped
ORDER BY a.attnum;
