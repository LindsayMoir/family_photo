-- Initial schema for family photo processing metadata.
-- Target database: photo_db

BEGIN;

CREATE TABLE IF NOT EXISTS scan_batches (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_dpi INTEGER,
    scanner_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source_dpi IS NULL OR source_dpi > 0)
);

CREATE TABLE IF NOT EXISTS sheet_scans (
    id BIGSERIAL PRIMARY KEY,
    scan_batch_id BIGINT NOT NULL REFERENCES scan_batches(id) ON DELETE RESTRICT,
    original_path TEXT NOT NULL UNIQUE,
    original_filename TEXT NOT NULL,
    content_hash TEXT,
    width_px INTEGER NOT NULL,
    height_px INTEGER NOT NULL,
    dpi_x INTEGER,
    dpi_y INTEGER,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (width_px > 0),
    CHECK (height_px > 0),
    CHECK (dpi_x IS NULL OR dpi_x > 0),
    CHECK (dpi_y IS NULL OR dpi_y > 0)
);

CREATE INDEX IF NOT EXISTS idx_sheet_scans_batch_id
    ON sheet_scans (scan_batch_id);

CREATE INDEX IF NOT EXISTS idx_sheet_scans_status
    ON sheet_scans (status);

CREATE TABLE IF NOT EXISTS photo_detections (
    id BIGSERIAL PRIMARY KEY,
    sheet_scan_id BIGINT NOT NULL REFERENCES sheet_scans(id) ON DELETE CASCADE,
    detection_method TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    region_type TEXT NOT NULL DEFAULT 'photo',
    contour_json JSONB,
    bbox_json JSONB NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    crop_path TEXT,
    ocr_text TEXT,
    ocr_engine TEXT,
    ocr_confidence DOUBLE PRECISION,
    accepted BOOLEAN NOT NULL DEFAULT FALSE,
    reviewed_by_human BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (confidence >= 0.0 AND confidence <= 1.0),
    CHECK (ocr_confidence IS NULL OR (ocr_confidence >= 0.0 AND ocr_confidence <= 1.0))
);

CREATE INDEX IF NOT EXISTS idx_photo_detections_sheet_scan_id
    ON photo_detections (sheet_scan_id);

CREATE INDEX IF NOT EXISTS idx_photo_detections_accepted
    ON photo_detections (accepted);

CREATE TABLE IF NOT EXISTS photos (
    id BIGSERIAL PRIMARY KEY,
    sheet_scan_id BIGINT NOT NULL REFERENCES sheet_scans(id) ON DELETE RESTRICT,
    accepted_detection_id BIGINT REFERENCES photo_detections(id) ON DELETE SET NULL,
    crop_index INTEGER NOT NULL,
    raw_crop_path TEXT,
    working_path TEXT,
    final_path TEXT,
    width_px INTEGER,
    height_px INTEGER,
    deskew_angle DOUBLE PRECISION,
    deskew_confidence DOUBLE PRECISION,
    rotation_degrees INTEGER,
    flip_mode TEXT,
    enhancement_version TEXT,
    export_disposition TEXT NOT NULL DEFAULT 'include',
    export_disposition_note TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (sheet_scan_id, crop_index),
    CHECK (width_px IS NULL OR width_px > 0),
    CHECK (height_px IS NULL OR height_px > 0),
    CHECK (
        deskew_confidence IS NULL OR
        (deskew_confidence >= 0.0 AND deskew_confidence <= 1.0)
    ),
    CHECK (
        rotation_degrees IS NULL OR
        rotation_degrees IN (0, 90, 180, 270)
    ),
    CHECK (
        export_disposition IN ('include', 'exclude_low_value', 'exclude_reject')
    )
);

CREATE INDEX IF NOT EXISTS idx_photos_sheet_scan_id
    ON photos (sheet_scan_id);

CREATE INDEX IF NOT EXISTS idx_photos_status
    ON photos (status);

CREATE TABLE IF NOT EXISTS photo_artifacts (
    id BIGSERIAL PRIMARY KEY,
    photo_id BIGINT NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    pipeline_stage TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_photo_artifacts_photo_id
    ON photo_artifacts (photo_id);

CREATE TABLE IF NOT EXISTS people (
    id BIGSERIAL PRIMARY KEY,
    display_name TEXT NOT NULL,
    canonical_name TEXT UNIQUE,
    birth_year INTEGER,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (birth_year IS NULL OR birth_year > 1800)
);

CREATE TABLE IF NOT EXISTS faces (
    id BIGSERIAL PRIMARY KEY,
    photo_id BIGINT NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    bbox_json JSONB NOT NULL,
    landmark_json JSONB,
    embedding JSONB,
    quality_score DOUBLE PRECISION,
    orientation_score DOUBLE PRECISION,
    detector_version TEXT NOT NULL,
    embedding_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (quality_score IS NULL OR (quality_score >= 0.0 AND quality_score <= 1.0)),
    CHECK (
        orientation_score IS NULL OR
        (orientation_score >= 0.0 AND orientation_score <= 1.0)
    )
);

CREATE INDEX IF NOT EXISTS idx_faces_photo_id
    ON faces (photo_id);

CREATE TABLE IF NOT EXISTS photo_people (
    id BIGSERIAL PRIMARY KEY,
    photo_id BIGINT NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    person_id BIGINT NOT NULL REFERENCES people(id) ON DELETE RESTRICT,
    face_id BIGINT REFERENCES faces(id) ON DELETE SET NULL,
    label_source TEXT NOT NULL,
    confidence DOUBLE PRECISION,
    verified_by_human BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0))
);

CREATE INDEX IF NOT EXISTS idx_photo_people_photo_id
    ON photo_people (photo_id);

CREATE INDEX IF NOT EXISTS idx_photo_people_person_id
    ON photo_people (person_id);

CREATE TABLE IF NOT EXISTS review_tasks (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    payload_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    resolution_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_review_tasks_status_priority
    ON review_tasks (status, priority, created_at);

CREATE TABLE IF NOT EXISTS processing_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    metrics_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_entity
    ON processing_jobs (entity_type, entity_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_status
    ON processing_jobs (status);

COMMIT;
