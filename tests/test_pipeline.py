import shutil
from pathlib import Path

from pipeline.run_demo import run_demo
from pipeline.validate import validate_vitals


def test_full_flow(tmp_path):
    # copy example mapping to temp dir
    base = Path.cwd()
    mapping_src = base / "pipeline" / "mappings" / "example_source_a.yaml"
    mapping_dst = tmp_path / "example_source_a.yaml"
    shutil.copy2(mapping_src, mapping_dst)

    # run demo using tmp_path as work_dir
    run_demo(mapping_path=str(mapping_dst), work_dir=str(tmp_path))

    std = tmp_path / "standardized"
    assert std.exists()
    assert (std / "vitals.parquet").exists()

    # run validation explicitly
    validate_vitals(std / "vitals.parquet", std, report_prefix="test_validation")
    assert (std / "test_validation.json").exists()
    assert (std / "test_validation_valid.parquet").exists()
