# Family Photo Project

This repository processes scanned family-photo sheets into reviewed digital-frame exports.

The system is built for an operator-driven workflow:

- ingest sheet scans into a batch
- detect candidate photos on each sheet
- crop, deskew, orient, and enhance each accepted photo
- export processed photos into `photos/exports/staging`
- review the staged JPGs visually
- make manual fixes in `photos/exports/staging/temp` when needed
- promote the final staged images into the frame export folders

The current workflow treats staging as the source of truth for operator review.

## Design

The application is a CLI-driven pipeline with a PostgreSQL database and a filesystem-based artifact store.

### Core Data Model

- `scan_batches`
  Groups an ingest run such as `book_1_600dpi`.
- `sheet_scans`
  One row per source scan image.
- `photo_detections`
  Candidate detections produced from each sheet.
- `photos`
  Accepted/created photo records that move through crop, deskew, orientation, enhancement, and export.
- `photo_artifacts`
  Files produced along the way, such as raw crops, deskewed images, enhanced images, staging exports, and final exports.
- `review_tasks`
  Human review queue for OCR, orientation, and export-audit issues.
- `ocr_requests`
  Deferred OCR requests when OCR is disabled or postponed.

### Pipeline Stages

The main pipeline stages are:

1. ingest
2. detect
3. crop
4. deskew
5. orient
6. enhance
7. frame export to staging

Each stage writes artifacts and updates database state. The orchestration entry points live in [src/pipeline/service.py](/mnt/d/GitHub/family_photo/src/pipeline/service.py).

### Filesystem Layout

Important directories under `photos/`:

- `book_*` or other source folders
  Original scanned sheet images.
- `crops/`
  Raw crop outputs by sheet.
- `derivatives/deskew`
  Deskewed intermediate images.
- `derivatives/orient`
  Orientation-corrected intermediate images.
- `derivatives/enhance`
  Enhanced working images.
- `exports/staging/landscape`
  Current staged landscape review images.
- `exports/staging/portrait`
  Current staged portrait review images.
- `exports/staging/temp`
  Operator-edited overrides that should be reapplied through the pipeline.
- `exports/frame_1920x1080`
  Final landscape exports.
- `exports/frame_1080x1920`
  Final portrait exports.

### Current Operator Workflow

The live workflow is intentionally conservative:

- process sheets in batches of `20`
- review the actual JPGs in staging, not just metadata
- make manual crop, deskew, and rotation fixes directly on images
- use `staging/temp` for operator overrides
- promote directly from staging once the images look correct

The export audit CSV still exists and can be generated, but it is no longer required for normal promotion.

## Environment

The CLI reads configuration from environment variables or `.env`.

Supported variables:

- `PHOTO_DB_URL` or `DATABASE_URL`
  PostgreSQL connection string. Must start with `postgresql://`
- `PHOTOS_ROOT`
  Photos root directory. Defaults to `photos`
- `LOG_LEVEL`
  One of `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`
- `ENV`
  Optional environment name. Defaults to `development`

Example `.env`:

```dotenv
PHOTO_DB_URL=postgresql://postgres:7377@localhost:5432/photo_db
PHOTOS_ROOT=photos
LOG_LEVEL=INFO
ENV=development
```

## Setup

### 1. Install dependencies

Use the Python environment you normally use for this repo, then install project requirements.

If you already have the environment, activate it first. The repo currently assumes commands are run like:

```bash
python src/cli.py show-config
```

### 2. Initialize the database

To inspect the base schema:

```bash
python src/cli.py init-db
python src/cli.py init-db --print-sql
```

Apply the schema and migrations using your normal PostgreSQL workflow.

### 3. Verify configuration

```bash
python src/cli.py show-config
```

## How To Run

### Ingest a source folder

```bash
python src/cli.py ingest --input photos/book_1_600dpi --batch book_1_600dpi
```

### Process the next 20 sheets

This advances sheet scans through detect, crop, deskew, orient, enhance, and staging export.

```bash
python src/cli.py run-batch --batch book_1_600dpi --limit 20
```

Important operational detail:

- `run-batch` also exports all currently export-ready photos into staging
- for tight review control, the preferred operator flow is to follow that with `stage-next-exports`

### Stage the next review batch

This is the preferred command for preparing the actual review slice.

```bash
python src/cli.py stage-next-exports --batch book_1_600dpi --limit 20
```

### Review staged images

Inspect:

- `photos/exports/staging/landscape`
- `photos/exports/staging/portrait`

The current process assumes you visually review these images directly.

### Make manual fixes

Put edited files in:

- `photos/exports/staging/temp`

Naming rules:

- `photo_<id>.jpg`
  Direct replacement for manual crop, manual deskew, manual rotation, or other single-image fixes.
- `photo_<id>_1.jpg`, `photo_<id>_2.jpg`, ...
  Manual split children for a merged image.

Examples:

- `photo_1147.jpg`
- `photo_1152_1.jpg`
- `photo_1152_2.jpg`

Then apply those edits:

```bash
python src/cli.py apply-manual-staging-edits
```

What that does:

- direct replacement files overwrite the working image for that `photo_id`
- split files create child photos through the manual split path
- all resulting images are rerun through the normal export pipeline and restaged

Operational note:

- `python src/cli.py promote-exports` now auto-applies any pending `staging/temp` edits before promoting
- if you use `--csv-path`, apply temp edits first so the CSV stays aligned with the staged set

### Optional: split-only review CSV

If you want a narrow CSV that only tracks whether a staged image should be split:

```bash
python src/cli.py write-split-review-csv
```

This writes:

- [photos/exports/staging/split_review.csv](/mnt/d/GitHub/family_photo/photos/exports/staging/split_review.csv)

The file has 2 columns:

- `image_name`
- `Split`

Operator rules:

- mark `Split` as `Y` only for images that should be auto-split
- leave `Split` as `N` for everything else

Then apply the selected splits:

```bash
python src/cli.py import-split-review-csv
```

What that does:

- reads only rows marked `Split=Y`
- runs those photos through the existing automatic split path
- restages the resulting photos for review
- leaves non-split images unchanged

### Optional: rebuild the audit snapshot

If you want a CSV snapshot of current staging:

```bash
python src/cli.py audit-exports --batch book_1_600dpi
```

This writes:

- [photos/exports/staging/export_audit.csv](/mnt/d/GitHub/family_photo/photos/exports/staging/export_audit.csv)

This file is now optional for the normal workflow.

### Promote reviewed staging images into final exports

Normal workflow:

```bash
python src/cli.py promote-exports
```

Current behavior:

- if you do not pass `--csv-path`, the command promotes all current staged exports
- if you pass `--csv-path`, the command uses the CSV-gated behavior

CSV-gated mode:

```bash
python src/cli.py promote-exports --csv-path photos/exports/staging/export_audit.csv
```

## Common Workflows

### Batch-of-20 operator loop

```bash
python src/cli.py run-batch --batch book_1_600dpi --limit 20
python src/cli.py stage-next-exports --batch book_1_600dpi --limit 20
python src/cli.py write-split-review-csv
python src/cli.py import-split-review-csv
python src/cli.py apply-manual-staging-edits
python src/cli.py promote-exports
```

In practice you will usually:

1. run `stage-next-exports`
2. visually inspect staging
3. optionally mark split candidates in `split_review.csv`
4. run `import-split-review-csv`
5. edit files in `staging/temp` for anything that still needs manual work
6. optionally run `apply-manual-staging-edits` to preview the restaged result before promotion
7. repeat until staging looks right
8. run `promote-exports`

### Manual split for a MERGE photo

If you split `photo_1152` into two operator-created children:

- `photos/exports/staging/temp/photo_1152_1.jpg`
- `photos/exports/staging/temp/photo_1152_2.jpg`

Then run:

```bash
python src/cli.py apply-manual-staging-edits
```

The original merged photo is excluded from exports, and only the child images should remain in staging.

### Requeue a final export set for review

If final portrait or landscape exports need to be pulled back into staging:

```bash
python src/cli.py requeue-final-exports --source-profile frame_1080x1920
python src/cli.py requeue-final-exports --source-profile frame_1920x1080
```

### Reset old scan lineage after rescanning

If a source directory has been rescanned and the old derived artifacts are invalid:

```bash
python src/cli.py reset-source-scans --source-dir book_1 --source-dir book_2 --dry-run
python src/cli.py reset-source-scans --source-dir book_1 --source-dir book_2
```

This removes downstream rows and artifacts for the targeted source directories only.

## Review And Audit Commands

List tasks:

```bash
python src/cli.py review list --limit 25
```

Show one task:

```bash
python src/cli.py review show --task-id 123
```

Dismiss open OCR tasks for a batch:

```bash
python src/cli.py review dismiss-ocr --batch book_1_600dpi
```

## OCR Behavior

OCR is disabled by default for the main processing commands:

- `detect`
- `process`
- `run-batch`
- `run-until-review`

To opt in:

```bash
python src/cli.py run-batch --batch book_1_600dpi --ocr
```

When OCR is off and a sheet still needs OCR-related handling, the system can queue work into `ocr_requests` while still letting the photo pipeline continue.

## Logs And Run Visibility

Tracked commands print lifecycle output:

- `status=started`
- `status=completed`
- `status=failed`

Tracked commands also emit:

- heartbeat log lines every 30 seconds
- per-run log files under `logs/run-YYYYMMDD-HHMMSS-PID.log`

This makes long-running commands observable without needing extra tooling.

## Tests

Run the full test suite:

```bash
pytest -q
```

## Important Current Behavior

- staging is the operator review area
- `staging/temp` is the operator override area
- promotion no longer requires the audit CSV unless you explicitly choose CSV-gated promotion
- `split_review.csv` is the narrow operator checklist for split-or-not decisions
- small review batches are the expected operating model
- `run-batch` is useful for processing sheets, but `stage-next-exports` is the better command for shaping the review slice

## Key Entry Points

- [src/cli.py](/mnt/d/GitHub/family_photo/src/cli.py)
  Main CLI.
- [src/pipeline/service.py](/mnt/d/GitHub/family_photo/src/pipeline/service.py)
  Batch orchestration.
- [src/frame_export/service.py](/mnt/d/GitHub/family_photo/src/frame_export/service.py)
  Staging and final export behavior.
- [src/frame_export/error_service.py](/mnt/d/GitHub/family_photo/src/frame_export/error_service.py)
  Next-batch staging, temp edits, and requeue utilities.
- [src/audit/fix_service.py](/mnt/d/GitHub/family_photo/src/audit/fix_service.py)
  Manual split and export-fix logic.
