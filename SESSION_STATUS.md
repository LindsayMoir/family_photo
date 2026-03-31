# SESSION_STATUS.md

Current handoff for Codex sessions in `/mnt/d/GitHub/family_photo`.

## Current Goal

Stabilize the rebuild pipeline that uses:

* `photos/tmp_rebuild_input/landscape` -> batch `rebuild_landscape`
* `photos/tmp_rebuild_input/portrait` -> batch `rebuild_portrait`

Primary user concern:

* long runs appeared to die silently
* user needs visible terminal status and persistent logs
* OCR should be off by default and deferred for later

## What Has Been Changed

### Run visibility

Implemented:

* explicit CLI lifecycle output:
  * `status=started`
  * `status=completed`
  * `status=failed`
* failure output includes:
  * `failure_kind`
  * `error_type`
  * `error_message`
* heartbeat logging every 30 seconds for tracked commands
* per-stage pipeline progress logging in `src/pipeline/service.py`
* per-run log files under `logs/run-YYYYMMDD-HHMMSS-PID.log`

Important detail:

* only tracked commands create run logs
* untracked commands like `show-config` should not create empty log files

### OCR behavior

Implemented:

* OCR is now disabled by default
* `detect`, `process`, `run-batch`, and `run-until-review` accept `--ocr` to opt in
* when OCR is disabled and a sheet would otherwise need OCR/text handling:
  * a fallback full-image `photo` detection is created
  * the sheet is queued in `public.ocr_requests`
  * the image can still continue through the photo pipeline

Migration added:

* `sql/migrations/004_ocr_requests.sql`

Live verification already done:

* `python src/cli.py detect --sheet-id 469` with default OCR-off:
  * created `public.ocr_requests` row for `sheet_scan_id=469`
  * created accepted `photo` detection, not `text`
* `python src/cli.py run-batch --sheet-id 469`
  * completed successfully
  * produced export

## Test Status

Last verified state:

* `pytest -q` passed with `30 passed`

## Git Status Caveat

This session could not `git add` / `git commit` / `git push` because `.git` is read-only in the current sandbox.

Observed error:

* `fatal: Unable to create '.git/index.lock': Read-only file system`

Likely fix outside repo:

* launch Codex with a less restrictive sandbox, for example via shell alias in `~/.bashrc`

## Pipeline State At Last Check

No pipeline job was running at the last check.

Database status last observed:

* `rebuild_landscape`
  * `97 detection_complete`
  * `97 detection_review_required`
  * `66 ingested`
* `rebuild_portrait`
  * `139 ingested`

Meaning:

* `rebuild_landscape` was partially processed and stopped
* `rebuild_portrait` had not started processing yet

## Important Investigation Result

The suspected “silent death” was not proven to be a deterministic crash.

What was observed:

* isolated `detect --sheet-id 468` completed in about 49 seconds
* isolated `detect --sheet-id 469` completed in about 44 seconds
* text-heavy sheets can spend a long time in detection/OCR-related logic
* many such sheets end in `detection_review_required`, so no new staging exports appear during those stretches

Interpretation:

* some of the “it died” signal was really “it is slow and not exporting”
* however, long supervisor runs still need further real-world validation after restart

## Recommended Next Step After Restart

1. Confirm the new session has Git-capable sandbox settings if commit/push is needed.
2. Read this file and `AGENTS.md`.
3. Resume with:
   * inspect current `rebuild_landscape` / `rebuild_portrait` DB counts
   * rerun `rebuild_landscape` with OCR still off by default
   * observe whether the new visibility/logging is sufficient
   * then run `rebuild_portrait`

## Useful Commands

Check batch status:

```bash
psql "postgresql://postgres:7377@localhost:5432/photo_db" -c "select sb.name, ss.status, count(*) from sheet_scans ss join scan_batches sb on sb.id=ss.scan_batch_id where sb.name in ('rebuild_landscape','rebuild_portrait') group by sb.name, ss.status order by sb.name, ss.status;"
```

Resume landscape:

```bash
python src/cli.py run-until-review --batch rebuild_landscape
```

Resume portrait:

```bash
python src/cli.py run-until-review --batch rebuild_portrait
```

Explicit OCR-enabled run if ever needed:

```bash
python src/cli.py run-until-review --batch rebuild_landscape --ocr
```
