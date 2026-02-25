import importlib
import sqlite3
from pathlib import Path

import pytest


def load_module(tmp_path: Path):
    # Import and redirect DB_PATH to a temp file so tests don't touch the repo
    import exercises as mod
    mod.DB_PATH = tmp_path / "exercises_test.db"
    importlib.reload(mod)
    mod.DB_PATH = tmp_path / "exercises_test.db"
    return mod


def setup_db(mod):
    conn = mod.connect(mod.DB_PATH)
    mod.create_schema(conn)
    mod.seed_courses(conn)
    conn.commit()
    return conn


def test_add_and_find_student(tmp_path):
    mod = load_module(tmp_path)
    conn = setup_db(mod)
    try:
        sid = mod.add_student(conn, "Ava", "ava@example.com")
        conn.commit()
        assert isinstance(sid, int) and sid > 0

        row = mod.find_student_by_email(conn, "ava@example.com")
        assert row is not None
        assert row["name"] == "Ava"
        assert row["email"] == "ava@example.com"

        missing = mod.find_student_by_email(conn, "missing@example.com")
        assert missing is None
    finally:
        conn.close()


def test_update_and_delete_student(tmp_path):
    mod = load_module(tmp_path)
    conn = setup_db(mod)
    try:
        sid = mod.add_student(conn, "Noah", "noah@example.com")
        conn.commit()

        updated = mod.rename_student(conn, sid, "Noah Kim")
        conn.commit()
        assert updated == 1

        row = mod.find_student_by_email(conn, "noah@example.com")
        assert row["name"] == "Noah Kim"

        deleted = mod.delete_student(conn, sid)
        conn.commit()
        assert deleted == 1

        row2 = mod.find_student_by_email(conn, "noah@example.com")
        assert row2 is None
    finally:
        conn.close()


def test_enrollments_join_and_transaction(tmp_path):
    mod = load_module(tmp_path)
    conn = setup_db(mod)
    try:
        s1 = mod.add_student(conn, "Ava", "ava@example.com")
        s2 = mod.add_student(conn, "Maya", "maya@example.com")
        conn.commit()

        cs205 = conn.execute("SELECT id FROM courses WHERE code = ?;", ("CS205",)).fetchone()["id"]
        cs310 = conn.execute("SELECT id FROM courses WHERE code = ?;", ("CS310",)).fetchone()["id"]

        # Duplicate enrollment -> rollback should leave no rows
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("BEGIN;")
            mod.enroll_student(conn, s1, cs205)
            mod.enroll_student(conn, s1, cs205)  # duplicate
            conn.execute("COMMIT;")

        conn.execute("ROLLBACK;")
        rows = mod.list_enrollments(conn)
        assert rows == []

        # Now enroll a few valid rows
        conn.execute("BEGIN;")
        mod.enroll_student(conn, s1, cs205)
        mod.enroll_student(conn, s1, cs310)
        mod.enroll_student(conn, s2, cs205)
        conn.execute("COMMIT;")

        rows = mod.list_enrollments(conn)
        assert len(rows) == 3

        # Check join columns exist
        first = rows[0]
        assert "student_name" in first.keys()
        assert "course_code" in first.keys()
        assert "course_title" in first.keys()

        # Check ordering: student_name, course_code
        pairs = [(r["student_name"], r["course_code"]) for r in rows]
        assert pairs == sorted(pairs)
    finally:
        conn.close()
