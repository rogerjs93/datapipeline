from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import joblib
import json
from datetime import datetime
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer


def f_to_c(f):
    try:
        return (f - 32.0) * 5.0 / 9.0
    except Exception:
        return np.nan


def apply_unit_conversions(df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
    # example: mapping may include unit info for fields
    for canon, src in mapping.get('mappings', {}).get('vitals', {}).items():
        if isinstance(src, dict) and src.get('unit') == 'F':
            col = src.get('column')
            if col in df.columns:
                df[canon] = df[col].apply(f_to_c)
    return df


def standardize_types(df: pd.DataFrame, schema_section: Dict[str, Any]) -> pd.DataFrame:
    # basic typing: convert datetimes and numerics
    for col, meta in schema_section.items():
        t = meta.get('type')
        if t in ('datetime', 'date') and col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        if t == 'numeric' and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def impute_missing(df: pd.DataFrame, policy: Dict[str, Any]) -> pd.DataFrame:
    default = policy.get('default', 'flag')
    if default == 'impute':
        # simple numeric mean imputation
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = df[col].fillna(df[col].mean())
    return df


def fit_transformers(df: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str], out_dir: Path, artifact_prefix: str = 'column_transformer') -> ColumnTransformer:
    """Fit and save a ColumnTransformer with StandardScaler for numeric and OneHotEncoder for categorical columns.

    artifact_prefix will be used to name the joblib file (e.g., column_transformer_{checksum}.joblib).
    Also writes a metadata JSON with input/output columns and timestamp.
    """
    transformers = []
    if numeric_cols:
        transformers.append(('num', StandardScaler(), numeric_cols))
    if categorical_cols:
        # sklearn newer versions use sparse_output instead of sparse
        try:
            transformers.append(('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_cols))
        except TypeError:
            transformers.append(('cat', OneHotEncoder(handle_unknown='ignore', sparse=False), categorical_cols))

    ct = ColumnTransformer(transformers, remainder='drop')
    ct.fit(df)
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = out_dir / f"{artifact_prefix}.joblib"
    joblib.dump(ct, artifact_path)

    # write metadata
    metadata = {
        'artifact': str(artifact_path.name),
        'numeric_columns': numeric_cols,
        'categorical_columns': categorical_cols,
        'fitted_at': datetime.utcnow().isoformat() + 'Z'
    }
    (out_dir / f"transformer_metadata_{artifact_prefix}.json").write_text(json.dumps(metadata, indent=2))
    return ct


def transform_with_artifacts(df: pd.DataFrame, transformer_path: Path) -> pd.DataFrame:
    ct = joblib.load(transformer_path)
    arr = ct.transform(df)
    # create column names for transformed output (best-effort)
    out_cols: List[str] = []
    for name, trans, cols in ct.transformers_:
        if name == 'num':
            out_cols.extend(cols)
        elif name == 'cat':
            # get feature names if encoder
            try:
                feat_names = trans.get_feature_names_out(cols)
                out_cols.extend(list(feat_names))
            except Exception:
                out_cols.extend([f"{c}_{i}" for i, c in enumerate(cols)])
    return pd.DataFrame(arr, columns=out_cols)
