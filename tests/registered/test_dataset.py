"""Registered (dataset creation) feature tests.

Dataset creation is a prerequisite for registering anything. The default
install must create a namespace and expose its registration formats.
"""

from __future__ import annotations

import uuid


def test_create_dataset(lite_app):
    name = "ds_" + uuid.uuid4().hex[:8]
    r = lite_app.post("/api/datasets", json={"name": name})
    assert r.status_code == 200, r.text
    try:
        body = r.json()
        assert body["dataset"] == name
        assert body["formats"]["a2a"] == "v0.0"
    finally:
        r = lite_app.delete(f"/api/datasets/{name}")
        assert r.status_code == 200, r.text
