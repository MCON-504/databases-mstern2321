import importlib
from pathlib import Path


def load_module(tmp_path: Path):
    import homework as mod
    mod.DB_PATH = tmp_path / "homework_test.db"
    importlib.reload(mod)
    mod.DB_PATH = tmp_path / "homework_test.db"
    return mod


def test_schema_and_inserts_and_queries(tmp_path):
    mod = load_module(tmp_path)
    conn = mod.connect(mod.DB_PATH)
    try:
        mod.create_schema(conn)
        conn.commit()

        s_ava = mod.add_student(conn, "Ava", "ava@example.com")
        s_noah = mod.add_student(conn, "Noah", "noah@example.com")
        s_maya = mod.add_student(conn, "Maya", "maya@example.com")

        a_q1 = mod.add_assignment(conn, "Quiz 1", 10)
        a_hw1 = mod.add_assignment(conn, "Homework 1", 100)
        a_mid = mod.add_assignment(conn, "Midterm", 200)

        mod.record_grade(conn, s_ava, a_q1, 9)
        mod.record_grade(conn, s_ava, a_hw1, 95)

        mod.record_grade(conn, s_noah, a_q1, 7)
        mod.record_grade(conn, s_noah, a_hw1, 88)

        mod.record_grade(conn, s_maya, a_q1, 10)
        mod.record_grade(conn, s_maya, a_hw1, 92)
        mod.record_grade(conn, s_maya, a_mid, 180)

        conn.commit()

        students = mod.list_students(conn)
        assert [s["name"] for s in students] == sorted([s["name"] for s in students])

        # Grade report percent correctness
        ava_report = mod.student_grade_report(conn, s_ava)
        assert len(ava_report) == 2
        for r in ava_report:
            assert "assignment_title" in r.keys()
            assert "score" in r.keys()
            assert "max_points" in r.keys()
            assert "percent" in r.keys()

        # Ava: 9/10=90.0 and 95/100=95.0
        percents = sorted([r["percent"] for r in ava_report])
        assert percents == [90.0, 95.0]

        # Leaderboard ordering by avg_percent desc (and correct values)
        board = mod.leaderboard(conn)
        assert len(board) == 3
        names = [r["student_name"] for r in board]
        assert set(names) == {"Ava", "Noah", "Maya"}

        # Maya: (100 + 92 + 90)/3 = 94.0
        # Ava: (90 + 95)/2 = 92.5
        # Noah: (70 + 88)/2 = 79.0
        by_name = {r["student_name"]: r["avg_percent"] for r in board}
        assert round(by_name["Maya"], 1) == 94.0
        assert round(by_name["Ava"], 1) == 92.5
        assert round(by_name["Noah"], 1) == 79.0

        avgs = [r["avg_percent"] for r in board]
        assert avgs == sorted(avgs, reverse=True)
    finally:
        conn.close()
