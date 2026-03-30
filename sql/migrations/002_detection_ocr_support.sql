BEGIN;

ALTER TABLE photo_detections
    ADD COLUMN IF NOT EXISTS region_type TEXT NOT NULL DEFAULT 'photo';

ALTER TABLE photo_detections
    ADD COLUMN IF NOT EXISTS crop_path TEXT;

ALTER TABLE photo_detections
    ADD COLUMN IF NOT EXISTS ocr_text TEXT;

ALTER TABLE photo_detections
    ADD COLUMN IF NOT EXISTS ocr_engine TEXT;

ALTER TABLE photo_detections
    ADD COLUMN IF NOT EXISTS ocr_confidence DOUBLE PRECISION;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'photo_detections_ocr_confidence_check'
    ) THEN
        ALTER TABLE photo_detections
            ADD CONSTRAINT photo_detections_ocr_confidence_check
            CHECK (ocr_confidence IS NULL OR (ocr_confidence >= 0.0 AND ocr_confidence <= 1.0));
    END IF;
END $$;

COMMIT;
