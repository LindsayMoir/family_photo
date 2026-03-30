# TEST_PLAN.md

## Goal
Provide a practical testing plan for the family photo pipeline so every code change is verified quickly and reliably.

## Test Strategy

The project should use a layered test strategy:
1. unit tests for small functions
2. integration tests for stage-to-stage behavior
3. end-to-end tests for representative pipeline runs
4. smoke tests for fast validation in active development

## Recommended Commands

Primary test command:
```bash
pytest
```

Useful variants:
```bash
pytest -q
pytest tests/unit
pytest tests/integration
pytest tests/e2e
pytest -k clustering
```

## Recommended Folder Structure

```text
project/
├── AGENTS.md
├── TEST_PLAN.md
├── pyproject.toml
├── src/
│   ├── detection/
│   ├── embedding/
│   ├── clustering/
│   ├── pipeline/
│   └── db/
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── test_preprocessing.py
    │   ├── test_detection.py
    │   ├── test_embedding.py
    │   ├── test_clustering.py
    │   └── test_db_models.py
    ├── integration/
    │   ├── test_pipeline_to_db.py
    │   └── test_detection_to_clustering.py
    ├── e2e/
    │   └── test_small_photo_batch.py
    └── fixtures/
        ├── images/
        └── embeddings/
```

## Unit Test Requirements

### Preprocessing
Verify:
- EXIF rotation is handled correctly
- invalid image paths raise clear errors
- normalization returns expected shape / type
- cropping utilities do not exceed image bounds

### Detection
Verify:
- known test image returns at least one face
- empty or unrelated image returns zero faces cleanly
- response format includes bounding box coordinates
- malformed detector output is handled safely

### Embedding
Verify:
- embedding output has expected dimensionality
- repeated run on same fixture is stable within tolerance
- clearly different faces are not falsely similar
- missing face crop raises a useful exception

### Clustering
Verify:
- known embeddings for same identity cluster together
- known embeddings for different identities separate
- noise points are marked correctly when applicable
- threshold changes do not silently break expected grouping

### Database
Verify:
- face records are created with required fields
- duplicate inserts are prevented or handled intentionally
- embeddings can be stored and read back correctly
- transaction rollback works on failure

## Integration Tests

### Detection -> Embedding
Use one or more known fixture images and verify:
- detected face crops can be passed to embedding stage
- each detected face gets exactly one embedding
- metadata survives stage transitions

### Embedding -> Clustering
Verify:
- embeddings created from fixture images produce stable cluster assignments
- cluster labels are mapped into database-ready records

### Pipeline -> Database
Verify:
- a small photo batch produces the expected number of image, face, and cluster records
- failures in one image do not necessarily abort the entire batch unless configured to do so

## End-to-End Test

Use a tiny curated dataset such as:
- 2 photos of person A
- 2 photos of person B
- 1 photo with no face
- 1 invalid or corrupt file

Verify:
- pipeline completes
- person A images group together
- person B images group together
- no-face image is handled gracefully
- corrupt file is logged and skipped or failed intentionally according to policy

## Fixtures Guidance

Keep fixtures:
- small
- stable
- version-controlled when licensing allows
- representative of real edge cases

Useful fixture categories:
- frontal face
- angled face
- low-light face
- group photo
- no-face image
- corrupt image

## Pytest Scaffolding Suggestion

### `tests/conftest.py`
Include shared fixtures for:
- sample image paths
- sample embeddings
- temporary database
- deterministic config / thresholds

Example ideas:
```python
import numpy as np
import pytest


@pytest.fixture
def sample_same_person_embeddings():
    return np.array([
        [0.10, 0.20, 0.30],
        [0.11, 0.19, 0.31],
    ])


@pytest.fixture
def sample_different_person_embeddings():
    return np.array([
        [0.10, 0.20, 0.30],
        [0.90, 0.80, 0.70],
    ])
```

### Example clustering test skeleton
```python
from sklearn.cluster import DBSCAN


def test_same_person_embeddings_cluster_together(sample_same_person_embeddings):
    labels = DBSCAN(eps=0.05, min_samples=1).fit(sample_same_person_embeddings).labels_
    assert labels[0] == labels[1]
```

## Fast Developer Workflow

During active coding, run:
```bash
pytest tests/unit -q
```

Before reporting completion, run:
```bash
pytest
```

## Review Gate Before Completion

A task should not be reported complete until:
- tests were added or updated
- full test suite was run
- failing tests were resolved
- obvious edge cases were reviewed

## Nice-to-Have Next Step

After the initial scaffold is in place, add:
- coverage reporting
- linting
- type checking
- CI execution on each push

Recommended later additions:
```bash
pytest --cov=src
ruff check .
mypy src
```
