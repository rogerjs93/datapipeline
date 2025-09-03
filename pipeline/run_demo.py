from pathlib import Path
import pandas as pd
from pipeline.ingest import load_mapping, read_source, save_standardized
from pipeline.normalize import apply_unit_conversions, standardize_types, impute_missing


BASE = Path(__file__).resolve().parent


def make_synthetic_csv(path: Path):
    df = pd.DataFrame({
        'PAT_ID': ['p1', 'p2'],
        'DOB': ['1980-01-01', '1990-06-15'],
        'Gender': ['M', 'F'],
        'ZIP': ['12345', '23456'],
        'MeasuredAt': ['2023-01-01T12:00:00Z', '2023-01-02T13:00:00Z'],
        'HR': [80, 72],
        'SBP': [120, 110],
        'DBP': [80, 70],
        'Temp_F': [98.6, 100.4],
        'Test': ['Glucose', 'WBC'],
        'Result': [5.6, 7.2],
        'Unit': ['mmol/L', '10^9/L']
    })
    df.to_csv(path, index=False)


def run_demo(mapping_path: str = None, work_dir: str = None):
    mappings_dir = BASE / 'mappings'
    if mapping_path:
        mapping = load_mapping(Path(mapping_path))
    else:
        mapping = load_mapping(mappings_dir / 'example_source_a.yaml')

    if work_dir:
        base_dir = Path(work_dir)
    else:
        base_dir = BASE

    # create synthetic source file
    source_table = Path(mapping['source']['table'])
    if not source_table.exists():
        make_synthetic_csv(source_table)

    raw = read_source(mapping, Path('.'))
    vitals = raw.get('vitals', pd.DataFrame())

    vitals = apply_unit_conversions(vitals, mapping)
    # load canonical schema for typing
    import yaml
    schema = yaml.safe_load((BASE / 'canonical_schema.yaml').read_text())
    vitals = standardize_types(vitals, schema.get('vitals', {}))
    vitals = impute_missing(vitals, mapping.get('missing_policy', {}))

    out = base_dir / 'standardized'
    out.mkdir(parents=True, exist_ok=True)

    # fit transformers on vitals numeric and categorical columns
    numeric_cols = [c for c, m in schema.get('vitals', {}).items() if m.get('type') == 'numeric' and c in vitals.columns]
    categorical_cols = [c for c, m in schema.get('vitals', {}).items() if m.get('type') in ('categorical', 'string') and c in vitals.columns]
    from pipeline.normalize import fit_transformers, transform_with_artifacts
    # compute a simple checksum of the mapping to version the transformer artifact
    import hashlib, yaml
    mapping_text = yaml.safe_dump(mapping)
    checksum = hashlib.sha1(mapping_text.encode('utf-8')).hexdigest()[:8]
    artifact_prefix = f"column_transformer_{checksum}"
    ct = fit_transformers(vitals, numeric_cols, categorical_cols, out, artifact_prefix=artifact_prefix)

    # transform using the saved artifact path (checksumed filename)
    transformed = transform_with_artifacts(vitals, out / f"{artifact_prefix}.joblib")

    save_standardized({'vitals': vitals, 'vitals_transformed': transformed}, out)
    # run validation and write report prefixed by checksum
    from pipeline.validate import validate_vitals
    report_prefix = f"validation_{checksum}"
    validate_vitals(out / 'vitals.parquet', out, report_prefix=report_prefix)

    # write an artifacts index mapping checksum -> artifacts
    import json
    artifacts = {
        'mapping_checksum': checksum,
        'transformer_artifact': f"{artifact_prefix}.joblib",
        'transformer_metadata': f"transformer_metadata_{artifact_prefix}.json",
        'validation_report': f"{report_prefix}.json",
        'validation_cleaned': f"{report_prefix}_valid.parquet",
    }
    (out / 'artifacts_index.json').write_text(json.dumps(artifacts, indent=2))

    # archive into artifacts/<checksum>/ and update master index
    from pipeline.artifacts import archive_artifacts
    archive_target = archive_artifacts(out, checksum)

    print('Demo complete. Standardized files, validation report, artifacts index, and archival written to:', out, '->', archive_target)


if __name__ == '__main__':
    run_demo()
