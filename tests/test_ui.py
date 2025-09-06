import os
import sys
import types

import yaml


def make_dummy_streamlit():
    m = types.ModuleType("streamlit")

    def noop(*a, **k):
        return None

    m.title = noop
    m.sidebar = types.SimpleNamespace(header=noop)
    m.file_uploader = lambda *a, **k: None
    m.selectbox = lambda *a, **k: None
    m.checkbox = lambda *a, **k: False

    class Empty:
        def text(self, *a, **k):
            return None

    m.empty = lambda: Empty()
    m.button = lambda *a, **k: False
    return m


def test_ui_import_and_mapping_helper(tmp_path):
    # Inject a dummy streamlit module so importing pipeline.ui doesn't require
    # the real package
    sys.modules["streamlit"] = make_dummy_streamlit()

    # Import the ui module
    import importlib

    ui = importlib.import_module("pipeline.ui")

    # list_mappings should return a list (may be empty)
    lst = ui.list_mappings()
    assert isinstance(lst, list)

    # Test prepare_mapping_for_uploaded_file replaces the first 'path' entry
    mapping = {"path": "old/path.csv", "nested": {"value": 1}}
    orig = tmp_path / "orig_mapping.yaml"
    with open(orig, "w", encoding="utf-8") as fh:
        yaml.safe_dump(mapping, fh)

    uploaded = tmp_path / "uploaded.csv"
    uploaded.write_text("a,b\n1,2", encoding="utf-8")

    out = ui.prepare_mapping_for_uploaded_file(str(orig), str(uploaded), str(tmp_path))
    assert os.path.exists(out)

    with open(out, "r", encoding="utf-8") as fh:
        mm = yaml.safe_load(fh)

    rel = os.path.relpath(str(uploaded), str(tmp_path))

    def contains_replaced_path(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "path" and v == rel:
                    return True
                if contains_replaced_path(v):
                    return True
        if isinstance(obj, list):
            for it in obj:
                if contains_replaced_path(it):
                    return True
        return False

    assert contains_replaced_path(mm)
