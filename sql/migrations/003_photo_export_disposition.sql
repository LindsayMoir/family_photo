BEGIN;

ALTER TABLE photos
    ADD COLUMN IF NOT EXISTS export_disposition TEXT NOT NULL DEFAULT 'include',
    ADD COLUMN IF NOT EXISTS export_disposition_note TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'photos_export_disposition_check'
    ) THEN
        ALTER TABLE photos
            ADD CONSTRAINT photos_export_disposition_check
            CHECK (export_disposition IN ('include', 'exclude_low_value', 'exclude_reject'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_photos_export_disposition
    ON photos (export_disposition);

COMMIT;
