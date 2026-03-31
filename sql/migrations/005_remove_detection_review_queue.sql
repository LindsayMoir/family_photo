BEGIN;

UPDATE sheet_scans
SET status = 'ingested',
    error_message = NULL,
    updated_at = NOW()
WHERE status = 'detection_review_required';

UPDATE review_tasks
SET status = 'dismissed',
    resolution_json = jsonb_build_object('action', 'obsolete_detection_review_queue_removed'),
    resolved_at = NOW()
WHERE task_type = 'review_detection'
  AND status IN ('open', 'in_progress');

COMMIT;
