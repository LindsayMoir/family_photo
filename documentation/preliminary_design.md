# Family Photo Processing System: Preliminary Design

## Document Purpose

This document defines a preliminary architecture for a Python-based system that processes scanned family photo album pages under WSL. It is written as an MVP design, but with enough rigor that a larger team could implement it without inventing key rules as they go.

The system is intended to:

- ingest scanned JPEG album-sheet images
- detect and extract each mounted photo into a separate JPEG
- detect text-only or mixed text regions as separate image regions
- straighten photos that are slightly skewed
- correct orientation, including 90/180/270 degree rotation
- allow manual flip correction when images appear mirrored
- apply conservative touch-ups
- OCR text regions and store recognized text in PostgreSQL
- support person labeling and later semi-automatic annotation
- store metadata in PostgreSQL database `photo_db`
- store image files on disk under `photos/`

## Executive Summary

The recommended MVP is a single Python CLI application with a staged processing pipeline and human review checkpoints. The pipeline should preserve original scans, produce deterministic derived artifacts, and persist provenance and review state in PostgreSQL.

Operator workflow should include a supervisor-style batch command that advances work automatically, exports any newly completed photos, and stops only when the next review item requires human input.
The supervisor should print categorized review counts such as `crop_detection`, `rotation`, and `ocr`, plus the exact next CLI command the operator should run to clear the next blocker.
Detection review should also include an audit for suspicious same-sheet geometry, such as overlapping accepted photo candidates or one unusually wide strip candidate that likely contains two adjacent photos.
It should also flag a wide candidate when one half of that candidate looks visually similar to another photo candidate on the same sheet, which is a strong sign of a duplicated wide crop rather than two legitimate independent photos.

The design deliberately avoids a web service, distributed workers, or full automatic face labeling in the first version. Those would add complexity before the hard parts are stable.

The highest-risk area is not the database or file layout. It is reliable extraction and orientation handling on real album scans that may contain:

- multiple photos on one sheet
- irregular spacing
- dark album backgrounds
- page borders
- tape or mounting corners
- handwritten notes
- touching or partially overlapping photos
- scans with mixed DPI

The MVP should therefore prioritize:

- immutable originals
- resumable processing
- explicit job and review states
- reprocessing support
- conservative automation

## Problem Statement

Each input file is typically a scan of a physical album page about 8.5 x 12 inches. A single scan may contain 1 to 6 individual photos. Each embedded photo may have one or more of these issues:

- needs to be cropped out of the larger scan
- is slightly rotated or skewed
- is sideways
- is upside down
- may be mirrored
- may require mild cleanup
- may contain one or more people who should eventually be identified

The system should convert these sheet scans into individually stored photo files, preserve traceability back to the original sheet, and build an annotation workflow that improves over time without sacrificing data quality.

## Goals

- Preserve original scans without modification
- Produce one output file per extracted photo
- Track provenance from original sheet to each derived photo
- Support deterministic reprocessing when algorithms improve
- Allow human review of uncertain extractions and orientations
- Build toward face-based search and annotation
- Keep the implementation understandable and maintainable

## Non-Goals for MVP

- Perfect automatic handling of all album layouts
- Full automatic identity labeling without human review
- Generative restoration, inpainting, or colorization
- Real-time processing requirements
- A web-first or service-heavy architecture

## Architectural Principles

### 1. Originals Are Immutable

Original scans should never be edited in place. All downstream outputs should be derived files with explicit lineage.

### 2. Processing Must Be Resumable

Every stage should be safe to retry. A crash mid-pipeline should not require manual database repair or hand deletion of files.

### 3. Decisions Need Confidence and Reviewability

Automatic decisions about crop boundaries, orientation, and identity should carry confidence. Low-confidence cases should produce review tasks instead of silent bad output.

### 4. Preserve Enough Metadata to Rebuild

The system should store source file identity, dimensions, geometry, processing versions, and review decisions so outputs can be regenerated later.

### 5. Conservative Automation Beats Aggressive Errors

The archive value of family photos is higher than the value of fast automation. Wrong rotations, bad crops, and poisoned face labels are more expensive than manual review.

### 6. Keep V1 Narrow

The first implementation should solve extraction, deskew, orientation, and persistence end to end. Face learning comes after that path is stable.

## Recommended Architecture

The MVP should be a single Python application running under WSL, with:

- a CLI entrypoint for batch operations
- PostgreSQL for metadata and review state
- filesystem storage for image binaries
- a local review workflow that can later be replaced by a lightweight UI

This is the simplest architecture that still supports:

- batch processing
- safe reprocessing
- metadata querying
- person search
- later face similarity search

## Why a CLI-First Approach

A CLI-first architecture is the right starting point because it:

- reduces implementation overhead
- is easy to automate from scripts
- works well in WSL
- makes batch ingestion straightforward
- does not force early UI decisions

If review throughput becomes a bottleneck, a small review UI can be added later on top of the same database and file model.

## Proposed Repository Layout

```text
family_photo/
  documentation/
    preliminary_design.md
  photos/
    originals/
    crops/
    finals/
    derivatives/
      thumbnails/
      review/
  sql/
    migrations/
    queries/
  src/
    cli.py
    config.py
    app_logging.py
    db/
    ingest/
    pipeline/
    detection/
    crop/
    deskew/
    orientation/
    enhance/
    faces/
    annotation/
    review/
    storage/
  tests/
    unit/
    integration/
    fixtures/
```

Notes:

- `pipeline/` should coordinate stage execution and retries.
- `storage/` should isolate file naming, path rules, and atomic writes.
- `db/` should isolate schema access and transaction boundaries.
- `review/` should own review task creation and resolution rules.

## Storage Model

### Filesystem as Binary Store

The filesystem should remain the source of truth for image binaries. PostgreSQL should store file paths, metadata, provenance, and annotations.

Suggested structure:

```text
photos/
  originals/{scan_batch_id}/{sheet_id}.jpg
  crops/{photo_id}/raw.jpg
  finals/{photo_id}/master.jpg
  exports/
    staging/
      landscape/
      portrait/
    frame_1920x1080/
    frame_1080x1920/
  derivatives/thumbnails/{photo_id}.jpg
  derivatives/review/{photo_id}.jpg
```

### Storage Rules

- never overwrite original scans
- write derived images atomically where possible
- keep final outputs separate from intermediate outputs
- route newly processed frame exports into `exports/staging/` first
- auto-apply best-guess orientation and deskew corrections before staging instead of blocking on orientation review
- record review confidence in the staging CSV so the operator reviews the staged image, not the intermediate uncertainty
- promote reviewed-good staging exports into the final frame folders only after review
- provide a single operator command to advance the next review slice all the way to a refreshed staging CSV
- after CSV review, importing the staging CSV should auto-apply all supported fixes before returning to the refreshed CSV
- allow OCR review to be bulk-dismissed when text correction is out of scope for the current pass
- keep file naming deterministic
- record all paths in the database
- make it possible to garbage-collect obsolete derivatives later

### Provenance Rules

Every derived photo should be traceable back to:

- scan batch
- original sheet scan
- detection record
- processing pipeline version
- review decisions that changed the result

## Configuration Model

Configuration should be explicit and environment-driven. Do not hardcode machine-specific paths or secrets.

Expected configuration areas:

- PostgreSQL connection settings
- root photo storage path
- processing profile defaults
- review thresholds
- face backend selection
- logging level

Recommended approach:

- `.env` for local configuration
- a typed Python config object loaded at process start
- validation at startup so invalid configuration fails fast

## Domain Model

There are four main domain entities:

1. sheet scans
2. extracted photos
3. people and face instances
4. review tasks and processing jobs

This separation matters. A sheet scan is not the same thing as a photo, and a photo is not the same thing as a face or a person label.

## Processing Pipeline

The pipeline should be staged and idempotent. Each stage should be able to:

- read prior state
- skip already completed work unless forced
- write output files
- persist status and metrics
- create review tasks when needed

Recommended stages:

1. ingest
2. detect sheet regions
3. crop
4. deskew
5. orient
6. enhance
7. detect faces
8. suggest labels
9. review and confirm

### Stage 1: Ingest

Input:

- one or more JPEG sheet scans

Responsibilities:

- validate file presence and readability
- extract image dimensions
- read DPI metadata if present
- calculate a content hash for deduplication if practical
- register a scan batch and sheet records
- move or copy files into managed storage

Outputs:

- `scan_batches` record
- `sheet_scans` record
- original image file under managed storage

Validation rules:

- reject unreadable files
- reject obviously invalid dimensions
- allow missing DPI metadata but record nulls explicitly
- flag potential duplicates for review instead of auto-dropping them

### Stage 2: Detect Sheet Regions

This stage detects meaningful regions within a larger sheet scan. A region may be:

- a photo
- a text document
- a mixed page that should be split into separate photo and text regions

For MVP, use classical computer vision rather than object detection models.

Expected approach:

- convert to grayscale
- denoise lightly
- run thresholding and or edge detection for photo-like rectangles
- use OCR-driven region search for text-heavy areas
- classify text regions as `printed`, `handwritten`, `mixed`, or `unknown`
- route printed text to a standard OCR backend and handwritten text to a handwriting-aware backend
- score candidate regions using geometry and OCR confidence
- classify regions as `photo` or `text`
- persist region crops for review and downstream processing

Output of this stage should be typed region records with geometry, confidence, and OCR text where applicable.

The initial implementation can keep both OCR paths on Tesseract-backed preprocessing, but the code should hide that behind an OCR interface so a stronger handwriting recognizer can be swapped in later without changing detection, review, or database flows.

When a stronger handwriting recognizer is enabled, it should only be used for `handwritten` and `mixed` text regions, and the persisted OCR metadata should record which backend produced the text so lower-confidence outputs can be audited or re-run later.

Detection review should be triggered when:

- no candidates are found
- more candidates than expected are found
- candidate confidence is low
- candidates overlap heavily
- detected shapes are too irregular
- OCR quality is too poor for the detected text region

### Stage 3: Crop

Cropping should be a separate stage from detection so the geometry can be reviewed or replaced without re-running the entire ingest pipeline.

### Stage 7: Frame Export

Digital picture frames should not consume the archival master files directly. Instead, the pipeline should generate a separate export layer with consistent output dimensions for the target device.

Responsibilities:

- read the latest processed `working_path` for each photo
- resize to a configured frame resolution such as `1920x1080` for landscape or `1080x1920` for portrait
- emit one frame delivery file per photo, chosen by the photo's orientation unless a specific export profile is explicitly requested
- fill the frame completely
- trim overflow from the long edge instead of adding presentation bands
- honor an operator-set export disposition so low-value or rejected photos stay in the archive but do not reach the digital frame
- persist export artifacts separately from archival masters

This stage should be fully repeatable so exports can be regenerated for a different frame profile later without touching the upstream crop, orientation, or enhancement outputs.

Responsibilities:

- take an accepted detection
- crop with a small configurable margin
- preserve resolution
- avoid clipping borders
- write the raw crop artifact

Output:

- `photos.raw_crop_path`

Crop stage rules:

- avoid automatic downscaling for master outputs
- allow thumbnails as separate derivatives
- preserve enough border to avoid cutting off the physical photo

### Stage 4: Deskew

Some extracted photos will be a few degrees off even after detection. This stage estimates and applies only a small angular correction.

Possible signals:

- rotated rectangle geometry
- edge alignment
- line detection along photo borders

Key rule:

Deskew should only correct small-angle error. It should not be responsible for deciding whether an image is sideways or upside down.

Recommended stored fields:

- `deskew_angle`
- `deskew_confidence`
- `deskew_method`
- processed artifact path

Review should be triggered when:

- the angle estimate is weak
- the crop has low visible border information
- correction would remove significant image area

### Stage 5: Orientation

This stage determines whether the photo should remain as-is or be rotated by 90, 180, or 270 degrees.

Orientation signals may include:

- EXIF transpose metadata
- detected face orientation
- text orientation
- simple composition heuristics

Important separation:

- deskew handles small angular corrections
- orientation handles cardinal rotations
- flip handling should remain explicit and rare

Supported actions:

- rotate 0
- rotate 90
- rotate 180
- rotate 270
- flip horizontal
- flip vertical

Policy:

- automatic rotation is allowed when confidence is high
- flip decisions should default to review unless there is strong evidence
- uncertain orientation should create a review task instead of silently guessing

### Stage 6: Enhancement

Enhancement should be conservative, versioned, and reversible by regeneration.

MVP-safe enhancement candidates:

- exposure normalization
- levels and contrast adjustment
- mild white balance correction
- gentle noise reduction
- dust speck suppression
- border cleanup when unambiguous

Do not automate in MVP:

- major scratch repair
- generative inpainting
- face restoration
- colorization

Every enhancement operation should record:

- enhancement pipeline version
- applied operations
- parameters if non-default
- output artifact path

### Stage 7: Face Detection

Face detection should start only after extraction and orientation are stable.

Responsibilities:

- detect faces in processed photos
- store bounding boxes and landmarks
- score face quality
- skip low-quality detections that are unusable

Important rule:

The system should distinguish between:

- a photo containing a person
- a face region detected in a photo
- an identity label attached to that face or photo

### Stage 8: Identity Suggestion

Identity learning should follow this progression:

1. detect faces
2. compute embeddings
3. cluster similar faces
4. allow a human to name clusters
5. suggest likely matches for new faces
6. auto-apply only above conservative thresholds

This avoids training the system on incorrect labels.

## Job and State Management

The current document needs stronger state rules than a simple status flag.

### Recommended Processing States

For sheets:

- `ingested`
- `detection_pending`
- `detection_complete`
- `detection_review_required`
- `crop_complete`
- `failed`

For photos:

- `crop_complete`
- `deskew_complete`
- `orientation_review_required`
- `orientation_complete`
- `enhancement_complete`
- `face_detection_complete`
- `ready_for_annotation`
- `failed`

For review tasks:

- `open`
- `in_progress`
- `resolved`
- `dismissed`

### Why This Matters

Without explicit state transitions, a team will struggle with:

- retry logic
- bulk reprocessing
- troubleshooting stuck items
- measuring pipeline throughput
- ensuring review actions happen in the right order

## Database Design

Database name:

- `photo_db`

The schema should stay simple, but it needs stronger constraints than the first draft. The goal is not full normalization. The goal is reliable traceability and safe updates.

### Table: `scan_batches`

Purpose:

- groups a batch of sheet scans from a single import operation

Suggested fields:

- `id`
- `name`
- `source_dpi`
- `scanner_notes`
- `created_at`

### Table: `sheet_scans`

Purpose:

- represents one original scanned album sheet

Suggested fields:

- `id`
- `scan_batch_id`
- `original_path`
- `original_filename`
- `content_hash`
- `width_px`
- `height_px`
- `dpi_x`
- `dpi_y`
- `status`
- `error_message` nullable
- `created_at`
- `updated_at`

Constraints and notes:

- `original_path` should be unique
- `content_hash` is useful for duplicate detection if implemented
- `status` should be validated against a known enum or constrained set

### Table: `photo_detections`

Purpose:

- stores candidate or accepted extraction geometry derived from a sheet scan

Suggested fields:

- `id`
- `sheet_scan_id`
- `detection_method`
- `pipeline_version`
- `contour_json`
- `bbox_json`
- `confidence`
- `accepted`
- `reviewed_by_human`
- `created_at`

Notes:

- multiple detection records may exist over time for the same sheet
- store geometry as JSON if using plain PostgreSQL types initially
- later optimization can introduce typed geometry storage if needed

### Table: `photos`

Purpose:

- represents one extracted logical photo

Suggested fields:

- `id`
- `sheet_scan_id`
- `accepted_detection_id`
- `crop_index`
- `raw_crop_path`
- `working_path`
- `final_path`
- `width_px`
- `height_px`
- `deskew_angle`
- `deskew_confidence`
- `rotation_degrees`
- `flip_mode`
- `enhancement_version`
- `status`
- `error_message` nullable
- `created_at`
- `updated_at`

Key notes:

- `working_path` is useful if multiple intermediate transforms exist
- `status` should represent the current lifecycle state
- `accepted_detection_id` preserves a direct link to the geometry source

### Table: `photo_artifacts`

Purpose:

- records all generated files for a photo instead of overloading `photos`

Suggested fields:

- `id`
- `photo_id`
- `artifact_type`
- `path`
- `pipeline_stage`
- `pipeline_version`
- `created_at`

Why add this table:

- it provides better provenance
- it supports regeneration and garbage collection
- it avoids adding too many path columns to `photos`

### Table: `people`

Purpose:

- stores canonical identity records

Suggested fields:

- `id`
- `display_name`
- `canonical_name`
- `birth_year` nullable
- `notes` nullable
- `created_at`
- `updated_at`

Notes:

- `canonical_name` should be unique if used
- allow incomplete people records early on

### Table: `faces`

Purpose:

- stores one detected face instance within a photo

Suggested fields:

- `id`
- `photo_id`
- `bbox_json`
- `landmark_json`
- `embedding` nullable
- `quality_score`
- `orientation_score`
- `detector_version`
- `embedding_version` nullable
- `created_at`

Notes:

- face detections should survive future identity relabeling
- embeddings should be versioned because models change

### Table: `photo_people`

Purpose:

- joins photos or faces to known people

Suggested fields:

- `id`
- `photo_id`
- `person_id`
- `face_id` nullable
- `label_source`
- `confidence`
- `verified_by_human`
- `created_at`
- `updated_at`

Semantics:

- `label_source` should distinguish `manual`, `suggested`, `auto_applied`, and `imported`
- `verified_by_human` should be explicit, not inferred

### Table: `review_tasks`

Purpose:

- stores work items for human review

Suggested fields:

- `id`
- `entity_type`
- `entity_id`
- `task_type`
- `status`
- `priority`
- `payload_json`
- `resolution_json` nullable
- `created_at`
- `resolved_at` nullable

Examples of task types:

- `review_detection`
- `review_orientation`
- `review_flip`
- `review_enhancement`
- `review_identity`

### Table: `processing_jobs`

Purpose:

- tracks batch and per-entity execution attempts

Suggested fields:

- `id`
- `job_type`
- `entity_type`
- `entity_id`
- `status`
- `attempt_count`
- `started_at`
- `finished_at` nullable
- `error_message` nullable
- `metrics_json` nullable

Why add it:

- separates operational execution from business entities
- makes debugging and retries much easier
- prevents overloading status fields with too much meaning

## Schema Guidance

The schema should follow these rules:

- parameterize all SQL
- use migrations for changes once schema work begins
- prefer nullable fields over inventing fake defaults
- add unique constraints where identity is stable
- store raw geometry and review payloads as JSON until query needs become clear
- keep enums simple at first unless they become difficult to manage

## Review Workflow

The original document treated review as necessary but under-specified the flow. A larger team would immediately ask who reviews what, in what order, and with what outputs.

### Review Types

1. detection review
2. orientation review
3. enhancement review
4. identity review

### Review Requirements

The reviewer must be able to:

- accept or reject a detected crop
- redraw or replace crop geometry if the detection is wrong
- rotate the photo to the correct orientation
- apply a horizontal flip when needed
- approve or reject enhancement output
- confirm, correct, or defer identity suggestions

### Review Ordering

Recommended order:

1. review detection
2. review orientation
3. review enhancement only if needed
4. review identity suggestions

Identity review should not happen before orientation is correct because sideways faces reduce detection and embedding quality.

## Face Recognition Strategy

This area needs strong guardrails.

### Rules

- never let unverified auto-labels become training truth immediately
- version embedding models and detector models
- preserve old embeddings if you may compare versions later
- prefer human-confirmed cluster labeling over direct self-training

### Recommended Learning Loop

1. process photos
2. detect faces
3. compute embeddings
4. find nearest existing faces or clusters
5. create identity suggestions
6. require human confirmation
7. only then mark labels as trusted

### Auto-Annotation Policy

Auto-annotation should remain opt-in and conservative. A useful team rule is:

- suggested labels are default behavior
- auto-applied labels require a very high confidence threshold
- auto-applied labels still remain reviewable later

## Mixed DPI Handling

Known current state:

- about 50 percent of the collection has been scanned
- of the scanned set, about half is 200 DPI
- the rest is 300 DPI
- future scans are expected to be 300 DPI

### Design Implications

- thresholds must scale with image dimensions, not fixed pixels
- contour area checks should be ratio-based
- deskew and cleanup settings may need DPI-aware tuning
- source DPI should always be recorded when available
- pipeline metrics should allow comparison by DPI cohort

### Operational Implications

If a later decision is made to re-scan 200 DPI material at 300 DPI, the system should allow:

- new sheet records for new scans
- linking superseded scans if desired
- re-running downstream processing without schema changes

## Error Handling and Recovery

The previous version did not say enough about failure behavior.

### Required Behaviors

- fail clearly when a file is unreadable or corrupt
- record stage-level errors in the database
- avoid partially committed state where metadata exists without expected files
- avoid files written without matching database records

### Recommended Tactics

- use transactions around metadata updates
- write derived files to temporary paths and rename atomically
- make each stage retryable
- keep previous successful artifacts until replacement succeeds

## Logging and Observability

A team implementation will need operational visibility, even for a local system.

Recommended logs:

- ingest start and completion
- detection counts and confidence summary
- crop failures
- deskew and orientation decisions
- review task creation
- face detection counts
- identity suggestion counts

Avoid logging:

- raw sensitive notes if present
- full face embeddings
- secrets or DB credentials

Useful metrics:

- average crops per sheet
- percent of sheets needing review
- percent of photos needing orientation review
- face detection hit rate
- identity suggestion acceptance rate

## Technology Recommendations

### Python Libraries

- OpenCV for contour detection, geometry, cropping, and deskew
- Pillow for image I/O and transform application
- psycopg for PostgreSQL access
- Alembic once database migrations begin

### Optional Later Additions

- `pgvector` for face embedding similarity search
- a lightweight review UI framework only after CLI review becomes painful

### Technology Decision Notes

- use sync Python for MVP
- avoid async until there is a real I/O concurrency need
- avoid introducing a heavy ORM unless it materially improves migration and query safety

## Suggested CLI Surface

The CLI should be explicit and batch-friendly.

Example commands:

```text
family-photo ingest --input /path/to/scans --batch 2026_03_album_a
family-photo detect --batch 2026_03_album_a
family-photo crop --sheet-id 123
family-photo deskew --photo-id 456
family-photo orient --photo-id 456
family-photo enhance --photo-id 456
family-photo review show --task-id 789
family-photo review export-ocr --task-id 789
family-photo review add-detection --task-id 789 --region-type photo --x1 100 --y1 100 --x2 500 --y2 500
family-photo review accept-detection --task-id 789 --detection-id 456
family-photo review next
family-photo review resolve --task-id 789
family-photo detect-faces --photo-id 456
family-photo suggest-labels --photo-id 456
family-photo reprocess --photo-id 456 --from-stage orient
```

CLI design rules:

- support dry-run where practical
- support batch and single-entity modes
- support force and skip semantics explicitly
- produce structured, readable console output

## Testing Strategy

The earlier draft did not specify test boundaries. A team will need this.

### Unit Tests

- geometry filtering
- crop naming and storage paths
- orientation decision rules
- review task generation
- status transition validation

### Integration Tests

- ingesting sample scans
- creating photo records and artifacts
- retrying failed stages
- resolving review tasks and re-running stages

### Fixture Strategy

Keep a small set of representative scans:

- clean sheet with 1 photo
- sheet with multiple photos
- skewed photo
- upside-down photo
- dark background album page
- difficult crop with notes or borders

## Security and Privacy Notes

This is a personal archive system, but privacy rules still matter.

- do not log secrets
- do not expose raw database credentials in code
- treat face embeddings as sensitive metadata
- keep any future remote model calls opt-in and clearly documented

## Recommended MVP Scope

The first implementation should focus on this path:

1. ingest sheet scans
2. detect candidate photos
3. crop accepted photos
4. apply deskew
5. support reviewable orientation correction
6. persist all metadata and artifacts

That is enough to prove the hard part of the system.

### Explicitly Defer

- embedding-based identity learning
- auto-annotation
- heavy restoration
- web UI
- distributed processing

## Delivery Plan

### Phase 1: Extraction MVP

- build storage and database schema
- implement ingest, detection, crop, deskew, and orientation
- implement review tasks for detection and orientation
- support deterministic reprocessing

### Phase 2: Enhancement and Review Improvements

- add conservative enhancement steps
- add artifact tracking
- improve batch review ergonomics

### Phase 3: Face Detection and Labeling

- add face detection
- add people and photo labeling workflows
- support manual and suggested labels

### Phase 4: Similarity Search and Semi-Automation

- add embeddings
- add similarity search
- add clustering
- add conservative identity suggestions

### Phase 5: UI and Operational Refinement

- add a lightweight review UI if justified
- improve observability
- add bulk reprocessing and admin tooling

## Open Questions

These questions should be answered before implementation starts in earnest:

- Do you want duplicate scan detection based on content hash?
- Should superseded rescans be linked to older scans in the database?
- Do you want all intermediate artifacts retained indefinitely, or only the latest plus originals?
- Is review expected to happen entirely in CLI for the first milestone?
- Which face backend is acceptable from a licensing and local-runtime standpoint?

## Final Recommendation

Build the system as a deterministic Python CLI pipeline with PostgreSQL-backed metadata, explicit job state, and mandatory review handling for uncertain cases. Do not begin with automatic face learning. First prove that the system can reliably turn messy scanned album pages into correctly oriented individual photo files with preserved provenance.

Once that path is stable, annotation and face-based assistance become much safer to add.
