import io
import os
import shlex
import subprocess

# ...existing code...
import threading
import time
import zipfile
from pathlib import Path

import streamlit as st
import yaml

# ...existing code...


def list_mappings():
    base = Path(__file__).resolve().parent / "mappings"
    if not base.exists():
        return []
    return sorted([str(p) for p in base.glob("*.yaml")])


def replace_first_path_in_mapping(mapping_obj, new_path):
    # Heuristic: look for the first key named 'path' in the YAML structure and
    # replace its value.
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


def prepare_mapping_for_uploaded_file(
    original_mapping_path: str, uploaded_filepath: str, dest_dir: str
) -> str:
    with open(original_mapping_path, "r", encoding="utf-8") as fh:
        mapping = yaml.safe_load(fh)

    # Try to replace the first 'path' found with the uploaded file path (relative)
    rel = os.path.relpath(uploaded_filepath, dest_dir)
    # Work on a copy
    modified = mapping

    def _replace(obj):
        if isinstance(obj, dict):
            for k in obj:
                if k == "path":
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


def run_pipeline_subprocess(
    mapping_path, work_dir, s3_bucket, s3_prefix, status_placeholder, control
):
    # Use a scrollable text area for logs via the placeholder
    status_placeholder.text_area(
        "Logs", value="Starting pipeline subprocess...\n", height=300
    )
    cmd = [
        shlex.quote("python"),
        "-u",
        "-m",
        "pipeline.cli",
    ]
    if mapping_path:
        cmd.extend(["--mapping", shlex.quote(str(mapping_path))])
    if work_dir:
        cmd.extend(["--work-dir", shlex.quote(str(work_dir))])
    if s3_bucket:
        cmd.extend(["--s3-bucket", shlex.quote(str(s3_bucket))])
    if s3_prefix:
        cmd.extend(["--s3-prefix", shlex.quote(str(s3_prefix))])

    # join to a single shell command for Windows Powershell compatibility in Streamlit
    shell_cmd = " ".join(cmd)

    try:
        # Start subprocess
        proc = subprocess.Popen(
            shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True
        )
        control["proc"] = proc
        # read lines and append to a buffer
        buf = []
        # prepare log file
        logs_dir = Path(work_dir) / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / f"run_{int(time.time())}.log"
        lf = open(log_file, "w", encoding="utf-8")
        for raw in proc.stdout:
            if control.get("cancel"):
                proc.terminate()
                buf.append("\n[Run cancelled by user]\n")
                break
            line = raw.decode("utf-8")
            # write to log file
            try:
                lf.write(line)
                lf.flush()
            except Exception:
                pass
            buf.append(line)
            # show last ~1000 chars to keep UI responsive
            text = "".join(buf[-200:])
            try:
                # replace the text area contents
                status_placeholder.text_area("Logs", value=text, height=300)
            except Exception:
                status_placeholder.text(line)
        proc.wait()
        try:
            lf.close()
        except Exception:
            pass
        if not control.get("cancel"):
            # append finished message
            try:
                current = "".join(buf[-200:])
                status_placeholder.text_area(
                    "Logs",
                    value=current
                    + "\nPipeline finished. Check the work directory for outputs.",
                    height=300,
                )
                # surface latest log file link in the UI
                try:
                    st.markdown("**Latest run log**")
                    st.markdown(f"- [{log_file.name}]({log_file.absolute().as_uri()})")
                except Exception:
                    pass
            except Exception:
                status_placeholder.text(
                    "Pipeline finished. Check the work directory for outputs."
                )
        control["proc"] = None
    except Exception as e:
        status_placeholder.text(f"Pipeline failed to start: {e}")


def main():
    st.title("Data Pipeline UI (lightweight)")

    st.sidebar.header("Run options")
    default_work = os.path.join(os.getcwd(), "pipeline_ui_runs")
    work_dir = st.sidebar.text_input("Work directory", value=default_work)
    os.makedirs(work_dir, exist_ok=True)

    st.sidebar.markdown("---")
    uploaded = st.file_uploader(
        "Upload a source CSV or Parquet file (optional)",
        type=["csv", "parquet"],
        accept_multiple_files=False,
    )

    mappings = list_mappings()
    selected = st.selectbox("Select mapping (optional)", options=["(none)"] + mappings)

    use_uploaded = st.checkbox(
        "If file uploaded, try to inject it into the selected mapping (heuristic)"
    )

    status = st.empty()

    # use session state to prevent concurrent runs
    if "running" not in st.session_state:
        st.session_state["running"] = False

    run_btn = st.button("Run pipeline", disabled=st.session_state["running"])
    if run_btn:
        st.session_state["running"] = True
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
                mapping_to_use = prepare_mapping_for_uploaded_file(
                    selected, uploaded_path, work_dir
                )
                status.text(f"Using modified mapping {mapping_to_use}")
            else:
                mapping_to_use = selected
                status.text(f"Using mapping {mapping_to_use}")
        else:
            mapping_to_use = None
            status.text("No mapping selected; the demo will create synthetic input.")

        s3_bucket = st.sidebar.text_input("S3 bucket (optional)")
        s3_prefix = st.sidebar.text_input("S3 prefix (optional)")

        # control dict shared between main thread and worker
        control = {"proc": None, "cancel": False}

        # Run the pipeline as a subprocess and stream stdout to the UI
        thread = threading.Thread(
            target=run_pipeline_subprocess,
            args=(
                mapping_to_use,
                work_dir,
                s3_bucket or None,
                s3_prefix or "",
                status,
                control,
            ),
        )
        thread.start()

        # Add a cancel button
        if st.button("Cancel run"):
            control["cancel"] = True
            # attempt graceful terminate then force kill
            p = control.get("proc")
            if p is not None:
                try:
                    p.terminate()

                    # wait a short time for process to exit
                    def _killer(proc):
                        try:
                            proc.wait(timeout=2)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass

                    threading.Thread(target=_killer, args=(p,), daemon=True).start()
                except Exception:
                    try:
                        p.kill()
                    except Exception:
                        pass

        # After the run finishes (not cancelled) try to show artifacts index
        def show_artifacts():
            try:
                art_path = Path(work_dir) / "standardized" / "artifacts_index.json"
                if art_path.exists():
                    data = yaml.safe_load(art_path.read_text(encoding="utf-8"))
                    st.markdown("**Artifacts index**")
                    for k, v in data.items():
                        p = (Path(work_dir) / "standardized" / v).absolute()
                        try:
                            uri = p.as_uri()
                            st.markdown(f"- **{k}**: [{v}]({uri})")
                        except Exception:
                            st.markdown(f"- **{k}**: {p}")
            except Exception:
                pass

        # run a lightweight waiter thread that calls show_artifacts when the
        # subprocess clears
        def waiter():
            while control.get("proc") is not None:
                time.sleep(0.5)
            if not control.get("cancel"):
                show_artifacts()

        def waiter_clear():
            while control.get("proc") is not None:
                time.sleep(0.5)
            st.session_state["running"] = False
            if not control.get("cancel"):
                show_artifacts()

        threading.Thread(target=waiter_clear, daemon=True).start()

        # Provide a button to create and download a ZIP of the standardized outputs
        try:
            std_dir = Path(work_dir) / "standardized"
            if std_dir.exists():
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    for p in std_dir.rglob("*"):
                        if p.is_file():
                            zf.write(p, arcname=p.relative_to(std_dir))
                zip_buf.seek(0)
                st.download_button(
                    "Download standardized artifacts (ZIP)",
                    data=zip_buf,
                    file_name="standardized_artifacts.zip",
                )
        except Exception:
            pass


if __name__ == "__main__":
    main()
