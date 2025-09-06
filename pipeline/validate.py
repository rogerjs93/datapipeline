import json
from pathlib import Path
from typing import List

import pandas as pd

from pipeline.models import Vitals


def validate_vitals(
    parquet_path: Path, out_dir: Path, report_prefix: str = "validation_report"
) -> None:
    df = pd.read_parquet(parquet_path)
    errors: List[dict] = []
    valid_rows = []

    for i, row in df.iterrows():
        data = row.to_dict()
        try:
            v = Vitals(**data)
            valid_rows.append(v.dict())
        except Exception as e:
            errors.append({"index": int(i), "error": str(e), "row": data})

    out_dir.mkdir(parents=True, exist_ok=True)

    # convert any pandas timestamps in errors to ISO strings
    def _serialize(obj):
        try:
            return obj.isoformat()
        except Exception:
            return obj

    for e in errors:
        if "row" in e:
            for k, v in e["row"].items():
                if hasattr(v, "isoformat"):
                    e["row"][k] = _serialize(v)

    report = {
        "input": str(parquet_path),
        "total_rows": len(df),
        "valid": len(valid_rows),
        "invalid": len(errors),
        "errors": errors,
    }

    report_path = out_dir / f"{report_prefix}.json"
    report_path.write_text(json.dumps(report, indent=2))

    # write cleaned parquet of valid rows. If none valid, write an empty file to
    # keep outputs stable
    valid_path = out_dir / f"{report_prefix}_valid.parquet"
    if valid_rows:
        pd.DataFrame(valid_rows).to_parquet(valid_path, index=False)
    else:
        # create empty dataframe with same columns as original canonical model fields
        empty = pd.DataFrame(columns=df.columns)
        empty.to_parquet(valid_path, index=False)


if __name__ == "__main__":
    base = Path(__file__).parent / "standardized"
    p = base / "vitals.parquet"
    validate_vitals(p, base)
