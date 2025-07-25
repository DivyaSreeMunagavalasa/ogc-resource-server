
-- Drop the default SERIAL behavior
ALTER TABLE metering
  ALTER COLUMN id DROP DEFAULT;

-- Drop the old sequence if it exists
DROP SEQUENCE IF EXISTS metering_id_seq;

-- Convert column to IDENTITY and restart from MAX(id) + 1
DO $$
DECLARE
  max_id BIGINT;
BEGIN
  SELECT COALESCE(MAX(id), 0) + 1 INTO max_id FROM metering;

  EXECUTE format('ALTER TABLE metering ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (START WITH %s)', max_id);
END $$;
