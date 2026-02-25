"""
demo.py — Databases + SQL basics demo (SQLite + Python)

Run:
  python demo.py

What this does:
  1) Creates (or recreates) a small SQLite database file: demo_school.db
  2) Creates tables: students, assignments, grades
  3) Inserts sample data
  4) Runs several queries (SELECT / JOIN / aggregation)
  5) Demonstrates parameterized queries (safe)
  6) Demonstrates a transaction rollback (all-or-nothing)
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable, Any

DB_PATH = Path("demo_school.db")


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Create a SQLite connection with helpful defaults.
    - row_factory makes rows behave like dicts (row["name"])
    - foreign_keys pragma enforces FK constraints in SQLite
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def exec_script(conn: sqlite3.Connection, sql: str) -> None:
    conn.executescript(sql)


def print_rows(title: str, rows: Iterable[sqlite3.Row]) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("-" * 80)
    rows = list(rows)
    if not rows:
        print("(no rows)")
        return

    # print as a simple table
    cols = rows[0].keys()
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}

    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


def reset_db() -> None:
    """
    Delete the database file if it exists, so we start fresh each time.
    :return: None
    """
    if DB_PATH.exists():
        DB_PATH.unlink()


def create_schema(conn: sqlite3.Connection) -> None:
    """
    Create the tables for our demo. We use a single executescript call with multiple statements for convenience.
    In order to run this multiple times without error, we start with DROP TABLE IF EXISTS for each table.
    We give each student a primary key id, and each grade references students and assignments by their ids.
    We also add some constraints:
    - max_points must be > 0
    - score must be >= 0
    - UNIQUE(student_id, assignment_id) to prevent duplicate grade entries for the same student+
        assignment

    :param conn:
    :return:
    """
    schema = """
    DROP TABLE IF EXISTS grades;
    DROP TABLE IF EXISTS assignments;
    DROP TABLE IF EXISTS students;

    CREATE TABLE students (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE
    );

    CREATE TABLE assignments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      max_points INTEGER NOT NULL CHECK (max_points > 0)
    );

    CREATE TABLE grades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      student_id INTEGER NOT NULL,
      assignment_id INTEGER NOT NULL,
      score INTEGER NOT NULL CHECK (score >= 0),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),

      FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
      FOREIGN KEY (assignment_id) REFERENCES assignments(id) ON DELETE CASCADE,

      -- prevent duplicate grade entries for the same student+assignment
      UNIQUE (student_id, assignment_id)
    );

    -- helpful index for joins / filtering
    CREATE INDEX idx_grades_student_id ON grades(student_id);
    CREATE INDEX idx_grades_assignment_id ON grades(assignment_id);
    """
    exec_script(conn, schema)


def seed_data(conn: sqlite3.Connection) -> None:
    """
    Insert some sample students, assignments, and grades.
    We insert students and assignments first so we can get their IDs to use as foreign keys when inserting grades.
    We use executemany for convenience to insert multiple rows at once.
    :param conn:
    :return:
    """
    students = [
        ("Ava", "ava@example.com"),
        ("Noah", "noah@example.com"),
        ("Maya", "maya@example.com"),
    ]
    assignments = [
        ("Quiz 1", 10),
        ("Homework 1", 100),
        ("Midterm", 200),
    ]
    # Insert students and assignments first so we can get their IDs for the grades table foreign keys
    conn.executemany("INSERT INTO students (name, email) VALUES (?, ?);", students)
    conn.executemany("INSERT INTO assignments (title, max_points) VALUES (?, ?);", assignments)

    # Fetch IDs so we can seed grades with correct foreign keys
    student_ids = {r["name"]: r["id"] for r in conn.execute("SELECT id, name FROM students;")}
    assignment_ids = {r["title"]: r["id"] for r in conn.execute("SELECT id, title FROM assignments;")}
    # Seed some grades (student_id, assignment_id, score)
    grades = [
        (student_ids["Ava"], assignment_ids["Quiz 1"], 9),
        (student_ids["Ava"], assignment_ids["Homework 1"], 95),
        (student_ids["Noah"], assignment_ids["Quiz 1"], 7),
        (student_ids["Noah"], assignment_ids["Homework 1"], 88),
        (student_ids["Maya"], assignment_ids["Quiz 1"], 10),
        (student_ids["Maya"], assignment_ids["Homework 1"], 92),
        (student_ids["Maya"], assignment_ids["Midterm"], 180),
    ]
    conn.executemany(
        "INSERT INTO grades (student_id, assignment_id, score) VALUES (?, ?, ?);",
        grades,
    )


def demo_basic_selects(conn: sqlite3.Connection) -> None:
    """
    Run a simple SELECT query to fetch all students and print the results.
    :param conn:
    :return:
    """
    rows = conn.execute(
        "SELECT id, name, email FROM students ORDER BY name;"
    ).fetchall()
    print_rows("All students", rows)


def demo_join(conn: sqlite3.Connection) -> None:
    """
    Run a JOIN query across grades, students, and assignments to produce a grade report.
    :param conn:
    :return:
    """
    rows = conn.execute(
        """
        SELECT s.name AS student, a.title AS assignment, g.score
        FROM grades g
        JOIN students s ON g.student_id = s.id
        JOIN assignments a ON g.assignment_id = a.id
        ORDER BY s.name, a.title;
        """
    ).fetchall()
    print_rows("Grades (JOIN across 3 tables)", rows)


def demo_aggregation(conn: sqlite3.Connection) -> None:
    """
    Run an aggregation query to calculate the average percentage score for each student across all their assignments.
    :param conn:
    :return:
    """
    rows = conn.execute(
        """
        SELECT
          s.name AS student,
          ROUND(AVG(1.0 * g.score / a.max_points) * 100, 1) AS avg_percent
        FROM grades g
        JOIN students s ON g.student_id = s.id
        JOIN assignments a ON g.assignment_id = a.id
        GROUP BY s.id
        ORDER BY avg_percent DESC;
        """
    ).fetchall()
    print_rows("Average percent per student (aggregation)", rows)


def demo_parameterized_query(conn: sqlite3.Connection) -> None:
    """
    Demonstrate how to use a parameterized query to safely include user input in a query.
    :param conn:
    :return:
    """
    print("\n" + "=" * 80)
    print("Parameterized query demo (SAFE)")
    print("-" * 80)

    # Imagine this comes from user input (form, query param, etc.)
    email_to_find = "maya@example.com"

    # ✅ Safe: placeholder with parameter tuple
    row = conn.execute(
        "SELECT id, name, email FROM students WHERE email = ?;",
        (email_to_find,),
    ).fetchone()

    print(f"Lookup by email={email_to_find!r} -> {dict(row) if row else None}")


def demo_transaction_rollback(conn: sqlite3.Connection) -> None:
    """
    Demonstrate how a transaction can be rolled back if something goes wrong.
    We'll try to insert a duplicate (student_id, assignment_id) grade which violates UNIQUE constraint.
    """
    print("\n" + "=" * 80)
    print("Transaction demo (rollback on error)")
    print("-" * 80)

    # Pick existing student+assignment pair
    existing = conn.execute(
        """
        SELECT student_id, assignment_id
        FROM grades
        LIMIT 1;
        """
    ).fetchone()

    assert existing is not None

    try:
        conn.execute("BEGIN;")
        conn.execute(
            "INSERT INTO grades (student_id, assignment_id, score) VALUES (?, ?, ?);",
            (existing["student_id"], existing["assignment_id"], 0),
        )
        conn.execute("COMMIT;")
    except sqlite3.IntegrityError as e:
        print("IntegrityError happened:", e)
        print("Rolling back...")
        conn.execute("ROLLBACK;")


def main() -> None:
    reset_db()
    conn = connect()
    try:
        create_schema(conn)
        seed_data(conn)
        conn.commit()

        demo_basic_selects(conn)
        demo_join(conn)
        demo_aggregation(conn)
        demo_parameterized_query(conn)
        demo_transaction_rollback(conn)

        # show that rollback kept the table consistent
        count = conn.execute("SELECT COUNT(*) AS n FROM grades;").fetchone()["n"]
        print(f"\nGrades row count after rollback demo: {count}")
        print("\nDone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
