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

## CI: Uploading artifacts to S3

A GitHub Actions workflow `upload_artifacts.yml` (in `.github/workflows/`) can sync the `pipeline/artifacts/` directory to an S3 bucket on pushes to `main`.

To enable this, add the following repository secrets in GitHub Settings → Secrets:

- `AWS_ACCESS_KEY_ID` — AWS access key
- `AWS_SECRET_ACCESS_KEY` — AWS secret
- `AWS_REGION` — AWS region (for example `us-east-1`)
- `S3_BUCKET` — bucket name where artifacts will be uploaded
- `S3_PREFIX` — optional prefix/path within the bucket (can be left empty)

The workflow will not try to upload if `S3_BUCKET` is not set.

## Run the UI (Streamlit)

To run the lightweight Streamlit UI locally from the project root:

```powershell
python -m pip install -r requirements.txt
streamlit run pipeline/ui.py
```

The UI allows uploading a source CSV/Parquet and selecting a mapping; it will run the pipeline and write outputs to the selected work directory.

