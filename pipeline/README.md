Pipeline README

Overview
This directory contains a minimal data ingestion & normalization pipeline demonstrating:
- Mapping-driven ingestion from CSV/Parquet
- Canonical schema (canonical_schema.yaml) and Pydantic models (`models.py`)
- Mapping templates (`mappings/`) to map source columns to canonical fields
- Normalization utilities (unit conversion, type standardization)
- Transformer fitting and artifacting (scikit-learn ColumnTransformer saved as joblib)
- Validation with Pydantic and a validation report
- Artifact archiving into `pipeline/artifacts/<checksum>/`

Quickstart (run from project root)

1) Install deps

```powershell
python -m pip install -r requirements.txt
```

2) Run the pipeline demo via CLI

```powershell
python -m pipeline.cli
```

3) Outputs
- `pipeline/standardized/` contains standardized parquet files, transformer artifact, validation report, and `artifacts_index.json`.
- `pipeline/artifacts/<checksum>/` contains archived artifacts for the run.

CLI
- `--mapping / -m` path to a mapping YAML
- `--work-dir / -w` alternate work directory

Next steps
- Add more unit conversions and code mapping support (LOINC/ICD)
- Add tests and CI
- Add remote artifact storage (S3)

