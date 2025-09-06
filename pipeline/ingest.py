from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml


def load_mapping(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_source(mapping: Dict[str, Any], base_path: Path) -> Dict[str, pd.DataFrame]:
    """Read source data according to mapping and return raw DataFrames.

    Supports CSV and Parquet.
    """
    source = mapping.get("source", {})
    fmt = source.get("format", "csv")
    table = source.get("table")

    full_path = base_path / table
    if fmt == "csv":
        df = pd.read_csv(full_path)
    elif fmt == "parquet":
        df = pd.read_parquet(full_path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    # split into logical tables based on mapping keys (demographics, vitals, labs)
    out = {}
    for table_name, cols in mapping.get("mappings", {}).items():
        # build a selection mapping: canonical_name -> source_column or dict
        select = {}
        for canon, src in cols.items():
            if isinstance(src, dict):
                select[canon] = src.get("column")
            else:
                select[canon] = src
        # filter only columns that exist to avoid KeyError
        available = {k: v for k, v in select.items() if v in df.columns}
        out[table_name] = df[list(available.values())].rename(
            columns={v: k for k, v in available.items()}
        )

    return out


def save_standardized(dfs: Dict[str, pd.DataFrame], out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        path = out_dir / f"{name}.parquet"
        df.to_parquet(path, index=False)
