"""
exercises.py — In-class practice: SQL fundamentals using sqlite3 (NO ORM yet)

Run:
  python exercises.py

Instructions:
  - Read each TODO and implement the missing code.
  - Run the file frequently and verify outputs.

Rules:
  - Use parameterized queries with ? placeholders (never string-concatenate user input).
  - Don’t change the table schema unless a TODO explicitly asks you to.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Iterable

DB_PATH = Path("../exercises.db")


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def reset_db() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE students (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT NOT NULL UNIQUE
        );

        CREATE TABLE courses (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT NOT NULL UNIQUE,
          title TEXT NOT NULL
        );

        CREATE TABLE enrollments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          student_id INTEGER NOT NULL,
          course_id INTEGER NOT NULL,
          enrolled_at TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
          FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE,
          UNIQUE(student_id, course_id)
        );
        """
    )


# ---------------------------
# TODO 1: INSERT (parameterized)
# ---------------------------
def add_student(conn: sqlite3.Connection, name: str, email: str) -> int:
    """
    Insert a student and return the new student id.

    TODO:
      - Use a parameterized INSERT
      - Return cursor.lastrowid
    """
    # cursor = conn.execute("INSERT ...", (...))
    # return cursor.lastrowid

    cursor = conn.execute("INSERT INTO students (name, email) values (?, ?);", (name, email))
    return cursor.lastrowid


# ---------------------------
# TODO 2: SELECT (one row)
# ---------------------------
def find_student_by_email(conn: sqlite3.Connection, email: str) -> Optional[sqlite3.Row]:
    """
    Return the student row for the given email, or None.

    TODO:
      - Use a parameterized SELECT
      - Use fetchone()

    """
    email_to_find = email
    row = conn.execute(
        "SELECT  id, name, email From students where email = ?;",
        (email_to_find,),
    ).fetchone()
    return row



# ---------------------------
# TODO 3: UPDATE
# ---------------------------
def rename_student(conn: sqlite3.Connection, student_id: int, new_name: str) -> int:
    """
    Update a student's name. Return number of rows updated (cursor.rowcount).

    TODO:
      - Use parameterized UPDATE
      - Return cursor.rowcount
    """
    cursor = conn.execute("UPDATE students SET name = ? WHERE id = ?;", (new_name, student_id))
    return cursor.rowcount


# ---------------------------
# TODO 4: DELETE
# ---------------------------
def delete_student(conn: sqlite3.Connection, student_id: int) -> int:
    """
    Delete a student by id. Return number of rows deleted.

    TODO:
      - Use parameterized DELETE
    """
    cursor = conn.execute("DELETE FROM students WHERE id = ?;", (student_id,))
    return cursor.rowcount

# ---------------------------
# TODO 5: JOIN query
# ---------------------------
def list_enrollments(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Return rows showing: student_name, course_code, course_title

    TODO:
      - Write a SELECT with JOIN across enrollments, students, courses
      - ORDER BY student_name, course_code
    """
    command = """
              SELECT s.name as student_name, c.code as course_code, c.title as course_title
              FROM enrollments e 
              JOIN students s ON e.student_id = s.id 
              JOIN courses c ON e.course_id = c.id
              ORDER BY s.name, c.code;
    """
    rows = conn.execute(command).fetchall()
    return rows

# ---------------------------
# TODO 6: transaction behavior
# ---------------------------
def enroll_student(conn: sqlite3.Connection, student_id: int, course_id: int) -> None:
    """
    Enroll a student in a course.

    TODO:
      - Use parameterized INSERT into enrollments
      - Do NOT commit here; caller controls commit/rollback.
    """
    conn.execute("INSERT INTO enrollments (student_id, course_id) VALUES (?, ?);", (student_id, course_id))

def seed_courses(conn: sqlite3.Connection) -> None:
    courses = [
        ("CS101", "Intro to Programming"),
        ("CS205", "Web Development"),
        ("CS310", "Data Structures"),
    ]
    conn.executemany("INSERT INTO courses (code, title) VALUES (?, ?);", courses)


def print_rows(title: str, rows: Iterable[sqlite3.Row]) -> None:
    rows = list(rows)
    print("\n" + "=" * 80)
    print(title)
    print("-" * 80)
    if not rows:
        print("(no rows)")
        return
    cols = rows[0].keys()
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


def main() -> None:
    reset_db()
    conn = connect()
    try:
        create_schema(conn)
        seed_courses(conn)
        conn.commit()

        # TODO 1-2: add 2 students, look one up
        # Expected: you should see ids printed and a dict-like row for the lookup.
        s1 = add_student(conn, "Ava", "ava@example.com")
        s2 = add_student(conn, "Noah", "noah@example.com")
        conn.commit()

        print(f"Inserted students: Ava={s1}, Noah={s2}")
        row = find_student_by_email(conn, "ava@example.com")
        print("Lookup ava@example.com ->", dict(row) if row else None)

        # TODO 6: enrollments in a transaction
        # We'll intentionally enroll the same pair twice to trigger UNIQUE constraint.
        # The expected behavior:
        #   - the second insert raises IntegrityError
        #   - we rollback
        #   - no enrollments are saved for that transaction block
        course_cs205 = conn.execute("SELECT id FROM courses WHERE code = ?;", ("CS205",)).fetchone()["id"]

        try:
            conn.execute("BEGIN;")
            enroll_student(conn, s1, course_cs205)
            enroll_student(conn, s1, course_cs205)  # duplicate on purpose
            conn.execute("COMMIT;")
        except sqlite3.IntegrityError as e:
            print("IntegrityError ->", e)
            print("Rolling back transaction...")
            conn.execute("ROLLBACK;")

        # TODO 5: list enrollments (should be empty due to rollback)
        rows = list_enrollments(conn)
        print_rows("Enrollments (should be empty after rollback)", rows)

        # Now do a valid transaction
        conn.execute("BEGIN;")
        enroll_student(conn, s1, course_cs205)
        conn.execute("COMMIT;")

        rows = list_enrollments(conn)
        print_rows("Enrollments after valid commit", rows)

        # TODO 3: rename a student
        updated = rename_student(conn, s2, "Noah Kim")
        conn.commit()
        print(f"rename_student rowcount={updated}")

        # Show updated students
        students = conn.execute("SELECT id, name, email FROM students ORDER BY id;").fetchall()
        print_rows("Students", students)

        # TODO 4: delete a student
        deleted = delete_student(conn, s1)
        conn.commit()
        print(f"delete_student rowcount={deleted}")

        students = conn.execute("SELECT id, name, email FROM students ORDER BY id;").fetchall()
        print_rows("Students after delete", students)

        print("\nAll done. If you finished early, add another course and test enrollments.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
