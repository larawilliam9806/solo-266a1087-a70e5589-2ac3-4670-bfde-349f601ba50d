from __future__ import annotations

import csv
import io
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import storage
from app.config import Config
from app.main import create_app


# ------------------------------------------------------------------ helpers


def make_config(tmp_path: Path, **overrides) -> Config:
    kwargs = dict(
        data_dir=tmp_path / "data",
        state_dir=tmp_path / "state",
        retry_delay=0.2,
        worker_poll=0.05,
    )
    kwargs.update(overrides)
    return Config(**kwargs)


def make_client(tmp_path: Path, **overrides) -> TestClient:
    return TestClient(create_app(make_config(tmp_path, **overrides)))


def csv_bytes(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["user_id", "page", "timestamp"])
    w.writerows(rows)
    return buf.getvalue().encode()


def upload(client: TestClient, date: str, rows):
    return client.post(
        f"/upload/{date}", files={"file": ("clicks.csv", csv_bytes(rows), "text/csv")}
    )


def wait_task(client: TestClient, task_id: str, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    body = None
    while time.time() < deadline:
        r = client.get(f"/erasure/{task_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] == "completed":
            return body
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not complete in time: {body}")


def wait_status(client: TestClient, task_id: str, want: str, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    body = None
    while time.time() < deadline:
        body = client.get(f"/erasure/{task_id}").json()
        if body["status"] == want:
            return body
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} never reached {want}: {body}")


def download_users(client: TestClient, date: str) -> set[str]:
    r = client.get(f"/dates/{date}/download")
    assert r.status_code == 200, r.text
    rows = list(csv.reader(io.StringIO(r.text)))
    return {row[0] for row in rows[1:]}


def download_rows(client: TestClient, date: str) -> list[list[str]]:
    r = client.get(f"/dates/{date}/download")
    assert r.status_code == 200, r.text
    return list(csv.reader(io.StringIO(r.text)))[1:]


def upload_ok(client: TestClient, date: str, rows) -> dict:
    r = upload(client, date, rows)
    assert r.status_code == 200, r.text
    return r.json()


# ------------------------------------------------------------------- tests


def test_health(tmp_path):
    with make_client(tmp_path) as client:
        assert client.get("/health").json() == {"status": "ok"}


def test_upload_and_download_roundtrip(tmp_path):
    with make_client(tmp_path) as client:
        r = upload(
            client,
            "2026-11-01",
            [
                ("alice", "/home", "2026-11-01T10:00:00Z"),
                ("bob", "/pricing", "2026-11-01 10:05:00+00:00"),
            ],
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["received"] == 2
        assert body["blocked"] == 0
        assert body["stored"] == 2

        r = client.get("/dates/2026-11-01/download")
        assert r.status_code == 200
        assert "alice" in r.text and "bob" in r.text

        # appending a second CSV to the same day keeps both
        upload(client, "2026-11-01", [("carol", "/about", "2026-11-01T11:00:00Z")])
        assert download_users(client, "2026-11-01") == {"alice", "bob", "carol"}

        # parquet download
        r = client.get("/dates/2026-11-01/download?format=parquet")
        assert r.status_code == 200
        table = pq.read_table(io.BytesIO(r.content))
        assert table.num_rows == 3
        assert str(table.schema.field("ts").type) == "timestamp[us, tz=UTC]"

        # listing
        dates = client.get("/dates").json()
        assert dates == [
            {"date": "2026-11-01", "files": 2, "rows": 3, "restricted": False}
        ]


def test_upload_validation(tmp_path):
    with make_client(tmp_path) as client:
        r = client.post(
            "/upload/2026-11-01",
            files={"file": ("x.csv", b"foo,bar\n1,2\n", "text/csv")},
        )
        assert r.status_code == 400

        r = client.post(
            "/upload/2026-13-40",
            files={"file": ("x.csv", csv_bytes([("a", "/p", "2026-11-01T00:00:00Z")]), "text/csv")},
        )
        assert r.status_code == 422

        # malformed rows are skipped and reported, valid rows are stored
        r = client.post(
            "/upload/2026-11-01",
            files={
                "file": (
                    "x.csv",
                    b"user_id,page,timestamp\nalice,/a,not-a-time\nbob,/b,2026-11-01T00:00:00Z\n",
                    "text/csv",
                )
            },
        )
        body = r.json()
        assert body["received"] == 2
        assert body["invalid"] == 1
        assert body["stored"] == 1


def test_erasure_blocks_new_uploads_immediately(tmp_path):
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        r = client.post("/erasure", json={"user_id": "alice"})
        assert r.status_code == 201
        task_id = r.json()["task_id"]

        # effective immediately, before the background worker finishes
        r = upload(
            client,
            "2026-11-02",
            [
                ("alice", "/b", "2026-11-02T00:00:00Z"),
                ("bob", "/c", "2026-11-02T00:01:00Z"),
            ],
        )
        body = r.json()
        assert body["received"] == 2
        assert body["blocked"] == 1
        assert body["stored"] == 1

        wait_task(client, task_id)
        assert download_users(client, "2026-11-02") == {"bob"}


def test_in_flight_upload_cannot_write_back_blocked_user(tmp_path):
    """An erasure accepted while an upload is being committed still wins."""
    with make_client(tmp_path) as client:
        service = client.app.state.service

        def accept_erasure_mid_commit():
            service.submit_erasure("alice")

        service.hooks.before_write = accept_erasure_mid_commit
        try:
            r = upload(
                client,
                "2026-11-01",
                [
                    ("alice", "/a", "2026-11-01T00:00:00Z"),
                    ("bob", "/b", "2026-11-01T00:01:00Z"),
                ],
            )
        finally:
            service.hooks.before_write = None

        body = r.json()
        assert body["blocked"] == 1
        assert body["stored"] == 1

        task = client.get("/erasure", params={"user_id": "alice"}).json()[0]
        wait_task(client, task["task_id"])
        assert download_users(client, "2026-11-01") == {"bob"}


def test_restricted_dates_pause_downloads_until_completion(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-01T00:00:00Z"),
                ("bob", "/b", "2026-11-01T01:00:00Z"),
            ],
        )
        upload(client, "2026-11-02", [("carol", "/c", "2026-11-02T00:00:00Z")])

        service = client.app.state.service
        # hold the worker so we can observe the restricted state deterministically
        gate = threading.Event()
        original_erase = service.erase_file

        def slow_erase(task, file_id):
            gate.wait(10)
            return original_erase(task, file_id)

        service.erase_file = slow_erase
        try:
            r = client.post("/erasure", json={"user_id": "alice"})
            task_id = r.json()["task_id"]
            assert r.json()["restricted_dates"] == ["2026-11-01"]

            # the affected date refuses new downloads (old links included)
            r = client.get("/dates/2026-11-01/download")
            assert r.status_code == 409
            # other dates are unaffected
            assert client.get("/dates/2026-11-02/download").status_code == 200
            # imports for other dates keep working
            assert upload(
                client, "2026-11-02", [("dave", "/d", "2026-11-02T01:00:00Z")]
            ).json()["stored"] == 1
        finally:
            gate.set()
            service.erase_file = original_erase

        wait_task(client, task_id)
        # restriction lifted once the rewrite finished
        r = client.get("/dates/2026-11-01/download")
        assert r.status_code == 200
        assert "alice" not in r.text
        assert "bob" in r.text


def test_erasure_rewrites_files_and_deletes_old_ones(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [("alice", "/a", "2026-11-01T00:00:00Z"), ("bob", "/b", "2026-11-01T00:01:00Z")],
        )
        upload(
            client,
            "2026-11-01",
            [("alice", "/x", "2026-11-01T02:00:00Z"), ("carol", "/y", "2026-11-01T02:01:00Z")],
        )
        data_dir = tmp_path / "data"
        before = {p.name for p in data_dir.rglob("part-*.parquet")}
        assert len(before) == 2

        r = client.post("/erasure", json={"user_id": "alice"})
        status = wait_task(client, r.json()["task_id"])
        assert status["files_total"] == 2
        assert status["files_done"] == 2

        # every old part file is physically gone
        after = {p.name for p in data_dir.rglob("part-*.parquet")}
        assert before.isdisjoint(after)
        assert not list(data_dir.rglob("*.tmp"))

        # other users' records survive
        assert download_users(client, "2026-11-01") == {"bob", "carol"}


def test_duplicate_submission_returns_same_task_and_blocks_forever(tmp_path):
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        r1 = client.post("/erasure", json={"user_id": "alice"})
        r2 = client.post("/erasure", json={"user_id": "alice"})
        assert r1.status_code == 201
        assert r2.status_code == 200
        assert r1.json()["task_id"] == r2.json()["task_id"]

        wait_task(client, r1.json()["task_id"])

        # a completed erasure blocks the user permanently
        r = upload(client, "2026-11-03", [("alice", "/x", "2026-11-03T00:00:00Z")])
        assert r.json()["blocked"] == 1
        assert r.json()["stored"] == 0
        assert client.get("/dates/2026-11-03/download").status_code == 404


def test_erasure_for_unknown_user_completes_and_blocks(tmp_path):
    with make_client(tmp_path) as client:
        r = client.post("/erasure", json={"user_id": "ghost"})
        assert r.status_code == 201
        status = wait_task(client, r.json()["task_id"])
        assert status["files_total"] == 0

        r = upload(
            client,
            "2026-11-01",
            [("ghost", "/a", "2026-11-01T00:00:00Z"), ("real", "/b", "2026-11-01T00:00:01Z")],
        )
        assert r.json()["blocked"] == 1
        assert r.json()["stored"] == 1


def test_interleaved_erasures_and_imports(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/1", "2026-11-01T00:00:00Z"),
                ("bob", "/2", "2026-11-01T00:01:00Z"),
                ("carol", "/3", "2026-11-01T00:02:00Z"),
            ],
        )
        upload(
            client,
            "2026-11-02",
            [("alice", "/4", "2026-11-02T00:00:00Z"), ("carol", "/5", "2026-11-02T00:01:00Z")],
        )
        upload(client, "2026-11-03", [("bob", "/6", "2026-11-03T00:00:00Z")])

        t1 = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        t2 = client.post("/erasure", json={"user_id": "bob"}).json()["task_id"]

        # imports interleave with the erasures; erased users are dropped,
        # everyone else keeps flowing in
        r = upload(
            client,
            "2026-11-01",
            [("alice", "/7", "2026-11-01T03:00:00Z"), ("dave", "/8", "2026-11-01T03:01:00Z")],
        )
        assert (r.json()["blocked"], r.json()["stored"]) == (1, 1)
        r = upload(
            client,
            "2026-11-02",
            [("bob", "/9", "2026-11-02T03:00:00Z"), ("erin", "/a", "2026-11-02T03:01:00Z")],
        )
        assert (r.json()["blocked"], r.json()["stored"]) == (1, 1)
        upload(client, "2026-11-04", [("frank", "/b", "2026-11-04T00:00:00Z")])

        wait_task(client, t1)
        wait_task(client, t2)

        # nothing of alice or bob remains anywhere, nobody else was lost
        assert download_users(client, "2026-11-01") == {"carol", "dave"}
        assert download_users(client, "2026-11-02") == {"carol", "erin"}
        assert client.get("/dates/2026-11-03/download").status_code == 404  # only bob
        assert download_users(client, "2026-11-04") == {"frank"}

        # disk and state agree: exactly the active files exist, nothing more
        service = client.app.state.service
        on_disk = {
            str(p.relative_to(tmp_path / "data"))
            for p in (tmp_path / "data").rglob("part-*.parquet")
        }
        active = {
            row["path"]
            for row in service.store.query("SELECT path FROM files WHERE status='active'")
        }
        assert on_disk == active


def test_failed_task_retries_and_eventually_completes(tmp_path, monkeypatch):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [("alice", "/a", "2026-11-01T00:00:00Z"), ("bob", "/b", "2026-11-01T00:01:00Z")],
        )

        real_write = storage.write_table_atomic
        calls = {"n": 0}

        def fail_once(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("simulated disk failure")
            return real_write(*args, **kwargs)

        monkeypatch.setattr(storage, "write_table_atomic", fail_once)

        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        status = wait_task(client, task_id)
        assert status["attempts"] >= 2  # first attempt failed, retry succeeded
        assert download_users(client, "2026-11-01") == {"bob"}


def test_write_failure_keeps_restrictions_and_restart_resumes(tmp_path, monkeypatch):
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        upload(
            client,
            "2026-11-01",
            [("alice", "/a", "2026-11-01T00:00:00Z"), ("bob", "/b", "2026-11-01T00:01:00Z")],
        )

        def always_fail(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(storage, "write_table_atomic", always_fail)
        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]

        status = wait_status(client, task_id, "failed")
        assert "disk full" in status["last_error"]
        # restrictions stay in place while the task cannot proceed
        assert client.get("/dates/2026-11-01/download").status_code == 409
        r = upload(client, "2026-11-02", [("alice", "/x", "2026-11-02T00:00:00Z")])
        assert r.json()["blocked"] == 1
    # shutdown with the task still failed

    # restart with a healthy disk: recovery picks the task up again
    monkeypatch.undo()
    app2 = create_app(config)
    with TestClient(app2) as client2:
        status = wait_task(client2, task_id)
        assert status["status"] == "completed"
        assert download_users(client2, "2026-11-01") == {"bob"}
        # still blocked forever
        r = upload(client2, "2026-11-05", [("alice", "/y", "2026-11-05T00:00:00Z")])
        assert r.json()["blocked"] == 1


def test_restart_with_pending_task_continues_erasure(tmp_path):
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        service = client.app.state.service
        # stop the worker before it can process anything
        service.worker.stop(timeout=2)
        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]

    app2 = create_app(config)
    with TestClient(app2) as client2:
        wait_task(client2, task_id)
        assert client2.get("/dates/2026-11-01/download").status_code == 404


def test_progress_endpoint_reports_counts(tmp_path):
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        upload(client, "2026-11-02", [("alice", "/b", "2026-11-02T00:00:00Z")])
        r = client.post("/erasure", json={"user_id": "alice"})
        task_id = r.json()["task_id"]
        status = wait_task(client, task_id)
        assert status["files_total"] == status["files_done"]
        assert status["restricted_dates"] == []
        assert status["last_error"] is None
        assert client.get("/erasure/does-not-exist").status_code == 404


# ------------------------------------------------------- quarantine / repair


def test_quarantined_rows_are_saved_and_queryable(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(
            client,
            "2026-11-01",
            [
                ("alice", "/home", "not-a-time"),
                ("bob", "/pricing", "2026-11-01T10:05:00Z"),
                ("carol", "/about", "also-bad"),
            ],
        )
        # quarantined rows still count as invalid; valid rows are stored
        assert body["received"] == 3
        assert body["invalid"] == 2
        assert body["stored"] == 1
        assert body["quarantined"] == 2
        ids = body["quarantine_ids"]
        assert len(ids) == 2

        rec = client.get(f"/quarantine/{ids[0]}").json()
        assert rec["user_id"] == "alice"
        assert rec["page"] == "/home"
        assert rec["date"] == "2026-11-01"
        assert rec["line_no"] == 2  # header is line 1
        assert rec["raw_timestamp"] == "not-a-time"
        assert rec["raw_line"] == "alice,/home,not-a-time"
        assert rec["status"] == "pending"
        assert rec["repaired_at"] is None
        assert client.get(f"/quarantine/{ids[1]}").json()["line_no"] == 4

        # list endpoint and filters
        all_recs = client.get("/quarantine").json()
        assert {r["record_id"] for r in all_recs} == set(ids)
        by_user = client.get("/quarantine", params={"user_id": "carol"}).json()
        assert [r["record_id"] for r in by_user] == [ids[1]]
        assert len(client.get("/quarantine", params={"status": "pending"}).json()) == 2
        assert client.get("/quarantine", params={"status": "repaired"}).json() == []

        assert client.get("/quarantine/does-not-exist").status_code == 404


def test_other_invalid_rows_are_not_quarantined(tmp_path):
    with make_client(tmp_path) as client:
        r = client.post(
            "/upload/2026-11-01",
            files={
                "file": (
                    "x.csv",
                    b"user_id,page,timestamp\n"
                    b",/a,2026-11-01T00:00:00Z\n"  # empty user_id
                    b"alice\n"  # missing columns
                    b"bob,/b,2026-11-01T00:00:00Z\n",
                    "text/csv",
                )
            },
        )
        body = r.json()
        assert body["received"] == 3
        assert body["invalid"] == 2
        assert body["stored"] == 1
        assert body["quarantined"] == 0
        assert body["quarantine_ids"] == []
        assert client.get("/quarantine").json() == []


def test_quarantine_line_numbers_match_the_file(tmp_path):
    with make_client(tmp_path) as client:
        r = client.post(
            "/upload/2026-11-01",
            files={
                "file": (
                    "x.csv",
                    b"user_id,page,timestamp\n\nalice,/a,bad-ts\n",
                    "text/csv",
                )
            },
        )
        body = r.json()
        assert body["quarantined"] == 1
        rec = client.get(f"/quarantine/{body['quarantine_ids'][0]}").json()
        assert rec["line_no"] == 3


def test_repair_writes_click_into_original_date(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(client, "2026-11-01", [("alice", "/home", "not-a-time")])
        rec_id = body["quarantine_ids"][0]
        assert body["stored"] == 0
        assert body["file_id"] is None
        assert client.get("/dates/2026-11-01/download").status_code == 404

        r = client.post(
            f"/quarantine/{rec_id}/repair",
            json={"timestamp": "2026-11-01T10:30:00Z"},
        )
        assert r.status_code == 200, r.text
        repaired = r.json()
        assert repaired["status"] == "repaired"
        assert repaired["repaired_ts"] == "2026-11-01T10:30:00+00:00"
        assert repaired["file_id"]

        # the click is served from the original upload date
        assert download_rows(client, "2026-11-01") == [
            ["alice", "/home", "2026-11-01T10:30:00+00:00"]
        ]
        assert client.get("/dates").json() == [
            {"date": "2026-11-01", "files": 1, "rows": 1, "restricted": False}
        ]

        # replaying the repair (e.g. after a lost response) must not duplicate
        r = client.post(
            f"/quarantine/{rec_id}/repair",
            json={"timestamp": "2026-11-01T10:30:00Z"},
        )
        assert r.status_code == 200
        assert r.json()["file_id"] == repaired["file_id"]
        assert download_rows(client, "2026-11-01") == [
            ["alice", "/home", "2026-11-01T10:30:00+00:00"]
        ]


def test_repair_rejects_invalid_timestamp_and_unknown_record(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(client, "2026-11-01", [("alice", "/home", "not-a-time")])
        rec_id = body["quarantine_ids"][0]

        r = client.post(f"/quarantine/{rec_id}/repair", json={"timestamp": "junk"})
        assert r.status_code == 422
        # the row stays pending and nothing was written
        assert client.get(f"/quarantine/{rec_id}").json()["status"] == "pending"
        assert client.get("/dates/2026-11-01/download").status_code == 404

        r = client.post(
            "/quarantine/nope/repair", json={"timestamp": "2026-11-01T00:00:00Z"}
        )
        assert r.status_code == 404


def test_upload_registers_valid_rows_and_quarantine_atomically(tmp_path, monkeypatch):
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        service = client.app.state.service

        def boom(*args, **kwargs):
            raise OSError("db unavailable")

        monkeypatch.setattr(service.store, "record_upload", boom)
        with pytest.raises(OSError):
            upload(
                client,
                "2026-11-01",
                [("alice", "/a", "2026-11-01T00:00:00Z"), ("bob", "/b", "bad-ts")],
            )
        # neither the valid rows nor the quarantine records were registered
        assert client.get("/dates").json() == []
        assert client.get("/quarantine").json() == []

    # restart: the orphaned part file is reconciled away, nothing lingers
    app2 = create_app(config)
    with TestClient(app2) as client2:
        assert list((tmp_path / "data").rglob("part-*.parquet")) == []
        assert client2.get("/quarantine").json() == []


def test_concurrent_repairs_never_duplicate(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(client, "2026-11-01", [("alice", "/a", "bad-ts")])
        rec_id = body["quarantine_ids"][0]
        service = client.app.state.service
        ts = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)

        results, errors = [], []

        def repair():
            try:
                results.append(service.repair_quarantine(rec_id, ts))
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=repair) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 8
        # every caller observes the same single repair
        assert {r["file_id"] for r in results} == {results[0]["file_id"]}
        assert download_rows(client, "2026-11-01") == [
            ["alice", "/a", "2026-11-01T10:00:00+00:00"]
        ]
        assert len(list((tmp_path / "data").rglob("part-*.parquet"))) == 1


def test_failed_repair_keeps_record_pending_and_retry_writes_once(tmp_path, monkeypatch):
    with make_client(tmp_path) as client:
        body = upload_ok(client, "2026-11-01", [("alice", "/a", "bad-ts")])
        rec_id = body["quarantine_ids"][0]

        real_write = storage.write_table_atomic
        calls = {"n": 0}

        def fail_once(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("simulated disk failure")
            return real_write(*args, **kwargs)

        monkeypatch.setattr(storage, "write_table_atomic", fail_once)
        with pytest.raises(OSError):
            client.post(
                f"/quarantine/{rec_id}/repair",
                json={"timestamp": "2026-11-01T10:00:00Z"},
            )
        # the pending row survives the failed repair
        assert client.get(f"/quarantine/{rec_id}").json()["status"] == "pending"

        r = client.post(
            f"/quarantine/{rec_id}/repair",
            json={"timestamp": "2026-11-01T10:00:00Z"},
        )
        assert r.status_code == 200
        assert download_rows(client, "2026-11-01") == [
            ["alice", "/a", "2026-11-01T10:00:00+00:00"]
        ]


def test_repair_registration_failure_leaves_no_orphan_after_restart(
    tmp_path, monkeypatch
):
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        body = upload_ok(client, "2026-11-01", [("alice", "/a", "bad-ts")])
        rec_id = body["quarantine_ids"][0]
        service = client.app.state.service

        def boom(*args, **kwargs):
            raise OSError("db unavailable")

        monkeypatch.setattr(service.store, "record_repair", boom)
        with pytest.raises(OSError):
            client.post(
                f"/quarantine/{rec_id}/repair",
                json={"timestamp": "2026-11-01T10:00:00Z"},
            )
        # record kept pending; the unregistered file is not part of the lake
        assert client.get(f"/quarantine/{rec_id}").json()["status"] == "pending"
        assert client.get("/dates/2026-11-01/download").status_code == 404
        assert len(list((tmp_path / "data").rglob("part-*.parquet"))) == 1

    # restart: recovery removes the orphaned file; the retry writes exactly once
    app2 = create_app(config)
    with TestClient(app2) as client2:
        assert list((tmp_path / "data").rglob("part-*.parquet")) == []
        r = client2.post(
            f"/quarantine/{rec_id}/repair",
            json={"timestamp": "2026-11-01T10:00:00Z"},
        )
        assert r.status_code == 200
        assert download_rows(client2, "2026-11-01") == [
            ["alice", "/a", "2026-11-01T10:00:00+00:00"]
        ]
        assert len(list((tmp_path / "data").rglob("part-*.parquet"))) == 1


def test_erasure_deletes_quarantine_records_and_blocks_repair(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(
            client,
            "2026-11-01",
            [("alice", "/a", "bad-ts"), ("bob", "/b", "also-bad")],
        )
        alice_rec, bob_rec = body["quarantine_ids"]

        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        wait_task(client, task_id)

        # alice's quarantined rows are gone and cannot be repaired anymore
        assert client.get(f"/quarantine/{alice_rec}").status_code == 404
        r = client.post(
            f"/quarantine/{alice_rec}/repair",
            json={"timestamp": "2026-11-01T10:00:00Z"},
        )
        assert r.status_code == 404

        # other users' rows are unaffected and still repairable
        r = client.post(
            f"/quarantine/{bob_rec}/repair",
            json={"timestamp": "2026-11-01T10:05:00Z"},
        )
        assert r.status_code == 200
        assert download_users(client, "2026-11-01") == {"bob"}


def test_bad_rows_of_blocked_users_are_not_quarantined(tmp_path):
    with make_client(tmp_path) as client:
        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        wait_task(client, task_id)

        body = upload_ok(
            client,
            "2026-11-01",
            [("alice", "/a", "bad-ts"), ("bob", "/b", "2026-11-01T00:00:00Z")],
        )
        assert body["invalid"] == 1  # alice's row still counts as invalid
        assert body["quarantined"] == 0  # but can never be quarantined/repaired
        assert body["stored"] == 1
        assert client.get("/quarantine").json() == []


def test_repaired_click_is_covered_by_future_erasure(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(
            client,
            "2026-11-01",
            [("carol", "/c", "bad-ts"), ("dave", "/d", "2026-11-01T00:00:00Z")],
        )
        rec_id = body["quarantine_ids"][0]
        r = client.post(
            f"/quarantine/{rec_id}/repair",
            json={"timestamp": "2026-11-01T09:00:00Z"},
        )
        assert r.status_code == 200
        assert download_users(client, "2026-11-01") == {"carol", "dave"}

        task_id = client.post("/erasure", json={"user_id": "carol"}).json()["task_id"]
        wait_task(client, task_id)
        # the repaired click is part of the lake and gets erased like any other
        assert download_users(client, "2026-11-01") == {"dave"}
        assert client.get(f"/quarantine/{rec_id}").status_code == 404


def test_repair_during_restriction_visible_once_download_resumes(tmp_path):
    with make_client(tmp_path) as client:
        body = upload_ok(
            client,
            "2026-11-01",
            [("alice", "/a", "2026-11-01T00:00:00Z"), ("carol", "/c", "bad-ts")],
        )
        rec_id = body["quarantine_ids"][0]

        service = client.app.state.service
        gate = threading.Event()
        original_erase = service.erase_file

        def slow_erase(task, file_id):
            gate.wait(10)
            return original_erase(task, file_id)

        service.erase_file = slow_erase
        try:
            task_id = client.post("/erasure", json={"user_id": "alice"}).json()[
                "task_id"
            ]
            # the date is restricted: downloads pause, but the repair still lands
            assert client.get("/dates/2026-11-01/download").status_code == 409
            r = client.post(
                f"/quarantine/{rec_id}/repair",
                json={"timestamp": "2026-11-01T09:00:00Z"},
            )
            assert r.status_code == 200
        finally:
            gate.set()
            service.erase_file = original_erase

        wait_task(client, task_id)
        # once the date is downloadable again, the repaired click is visible
        assert download_users(client, "2026-11-01") == {"carol"}
