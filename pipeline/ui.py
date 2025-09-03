import os
import yaml
import tempfile
import threading
from pathlib import Path

import streamlit as st

from pipeline import run_demo


def list_mappings():
    base = Path(__file__).resolve().parent / "mappings"
    if not base.exists():
        return []
    return sorted([str(p) for p in base.glob("*.yaml")])


def replace_first_path_in_mapping(mapping_obj, new_path):
    # Heuristic: look for the first key named 'path' in the YAML structure and replace its value.
    if isinstance(mapping_obj, dict):
        for k, v in mapping_obj.items():
            if k == "path":
                return new_path
            else:
                res = replace_first_path_in_mapping(v, new_path)
                if res is not None:
                    mapping_obj[k] = res if not isinstance(v, (list, dict)) else v
                    return mapping_obj
    if isinstance(mapping_obj, list):
        for i, item in enumerate(mapping_obj):
            res = replace_first_path_in_mapping(item, new_path)
            if res is not None:
                mapping_obj[i] = res if not isinstance(item, (list, dict)) else item
                return mapping_obj
    return None


def prepare_mapping_for_uploaded_file(original_mapping_path: str, uploaded_filepath: str, dest_dir: str) -> str:
    with open(original_mapping_path, "r", encoding="utf-8") as fh:
        mapping = yaml.safe_load(fh)

    # Try to replace the first 'path' found with the uploaded file path (relative)
    rel = os.path.relpath(uploaded_filepath, dest_dir)
    # Work on a copy
    modified = mapping
    def _replace(obj):
        if isinstance(obj, dict):
            for k in obj:
                if k == 'path':
                    obj[k] = rel
                    return True
            for k in obj:
                if _replace(obj[k]):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if _replace(item):
                    return True
        return False

    _replace(modified)

    out_path = os.path.join(dest_dir, "mapping_used.yaml")
    with open(out_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(modified, fh)
    return out_path


def run_pipeline_in_thread(mapping_path, work_dir, status_placeholder):
    try:
        status_placeholder.text("Running pipeline... (this may take a few seconds)")
        run_demo.run_demo(mapping_path=mapping_path, work_dir=work_dir)
        status_placeholder.text("Pipeline finished. Check the work directory for outputs.")
    except Exception as e:
        status_placeholder.text(f"Pipeline failed: {e}")


def main():
    st.title("Data Pipeline UI (lightweight)")

    st.sidebar.header("Run options")
    default_work = os.path.join(os.getcwd(), "pipeline_ui_runs")
    work_dir = st.sidebar.text_input("Work directory", value=default_work)
    os.makedirs(work_dir, exist_ok=True)

    st.sidebar.markdown("---")
    uploaded = st.file_uploader("Upload a source CSV or Parquet file (optional)", type=["csv", "parquet"], accept_multiple_files=False)

    mappings = list_mappings()
    selected = st.selectbox("Select mapping (optional)", options=["(none)"] + mappings)

    use_uploaded = st.checkbox("If file uploaded, try to inject it into the selected mapping (heuristic)")

    status = st.empty()

    if st.button("Run pipeline"):
        # Save upload
        uploaded_path = None
        if uploaded is not None:
            up_dir = os.path.join(work_dir, "uploads")
            os.makedirs(up_dir, exist_ok=True)
            uploaded_path = os.path.join(up_dir, uploaded.name)
            with open(uploaded_path, "wb") as fh:
                fh.write(uploaded.getbuffer())
            status.text(f"Saved uploaded file to {uploaded_path}")

        # Prepare mapping
        mapping_to_use = None
        if selected != "(none)":
            if uploaded_path and use_uploaded:
                mapping_to_use = prepare_mapping_for_uploaded_file(selected, uploaded_path, work_dir)
                status.text(f"Using modified mapping {mapping_to_use}")
            else:
                mapping_to_use = selected
                status.text(f"Using mapping {mapping_to_use}")
        else:
            mapping_to_use = None
            status.text("No mapping selected; the demo will create synthetic input.")

        thread = threading.Thread(target=run_pipeline_in_thread, args=(mapping_to_use, work_dir, status))
        thread.start()


if __name__ == "__main__":
    main()
