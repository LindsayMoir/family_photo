BEGIN;

CREATE TABLE IF NOT EXISTS ocr_requests (
    id BIGSERIAL PRIMARY KEY,
    sheet_scan_id BIGINT NOT NULL UNIQUE REFERENCES sheet_scans(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    request_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ocr_requests_status
    ON ocr_requests (status, created_at);

COMMIT;
