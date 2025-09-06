import json
import shutil
from pathlib import Path


def archive_artifacts(standardized_dir: Path, checksum: str) -> Path:
    """Copy standardized artifacts into artifacts/<checksum>/ and update index."""
    artifacts_root = standardized_dir.parent / "artifacts"
    target = artifacts_root / checksum
    target.mkdir(parents=True, exist_ok=True)

    # copy all files from standardized_dir into target
    for p in standardized_dir.glob("*"):
        if p.is_file():
            shutil.copy2(p, target / p.name)

    # update master index
    master = artifacts_root / "artifacts_index_master.json"
    if master.exists():
        data = json.loads(master.read_text())
    else:
        data = {}

    data[checksum] = [f.name for f in target.iterdir()]
    master.write_text(json.dumps(data, indent=2))
    return target
