Audit model trial output

Each case folder contains:
- before/: the current staged export
- manual_expected/: a non-destructive preview based on your current CSV issue code
- model_predicted/: a non-destructive preview based on the vision model suggestion
- summary.json: manual label, model label, confidence, and agreement

Supported issue previews: MERGE, R180, RR90, RL90, CROP, SKEW, DELETE
CROP and SKEW previews use manual notes when available.
Agreement is scored against the first manual issue code for multi-code rows.