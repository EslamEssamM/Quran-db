import os
import sqlite3

DB_PATH = 'quran_arabic.db'


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def ensure_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Performance / integrity
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=NORMAL')
    cur.execute('PRAGMA foreign_keys=ON')

    # Add page_number and line_number to Suras if missing
    if not column_exists(conn, 'Suras', 'page_number'):
        cur.execute('ALTER TABLE Suras ADD COLUMN page_number INTEGER')
    if not column_exists(conn, 'Suras', 'line_number'):
        cur.execute('ALTER TABLE Suras ADD COLUMN line_number INTEGER')
    conn.commit()


def update_suras_page_line(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Populate Suras.page_number and Suras.line_number.

    Preferred: Use Lines table by matching Lines.sura_id and taking the earliest
    occurrence (lowest page_id, then lowest line_number on that page). Resolve to
    Pages.page_number when available.

    Fallback: If Lines is missing/empty, use first ayah (ayat_number=1) per
    surah and derive from Words/Pages as before.

    Returns (count_page_filled, count_line_filled).
    """
    cur = conn.cursor()

    # Detect Lines table
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Lines'")
    has_lines = cur.fetchone() is not None

    if has_lines:
        # Update using Lines with deterministic first-line per sura
        cur.execute(
            '''
            WITH first_line AS (
                SELECT l.sura_id, l.page_id, l.line_number
                FROM Lines l
                JOIN (
                    SELECT sura_id, MIN(page_id) AS min_page
                    FROM Lines
                    WHERE sura_id IS NOT NULL
                    GROUP BY sura_id
                ) mp ON mp.sura_id = l.sura_id AND mp.min_page = l.page_id
                JOIN (
                    SELECT sura_id, page_id, MIN(line_number) AS min_line
                    FROM Lines
                    WHERE sura_id IS NOT NULL
                    GROUP BY sura_id, page_id
                ) ml ON ml.sura_id = l.sura_id AND ml.page_id = l.page_id AND ml.min_line = l.line_number
                GROUP BY l.sura_id
            ),
            resolved AS (
                SELECT fl.sura_id,
                       COALESCE(p.page_number, fl.page_id) AS page_number,
                       fl.line_number
                FROM first_line fl
                LEFT JOIN Pages p ON p.page_id = fl.page_id
            )
            UPDATE Suras
               SET page_number = (
                       SELECT r.page_number FROM resolved r WHERE r.sura_id = Suras.sura_id
                   ),
                   line_number = (
                       SELECT r.line_number FROM resolved r WHERE r.sura_id = Suras.sura_id
                   )
            '''
        )
        conn.commit()
    else:
        # Fallback: first ayah + words
        cur.execute(
            '''
            WITH first_ayats AS (
                SELECT a.sura_id, a.ayat_id, a.page_id
                FROM Ayats a
                WHERE a.ayat_number = 1
            ),
            word_stats AS (
                SELECT w.ayat_id,
                       MIN(w.page_number) AS w_page,
                       MIN(w.line_number) AS w_line
                FROM Words w
                GROUP BY w.ayat_id
            ),
            derived AS (
                SELECT fa.sura_id,
                       COALESCE(ws.w_page, p.page_number) AS page_number,
                       ws.w_line AS line_number
                FROM first_ayats fa
                LEFT JOIN word_stats ws ON ws.ayat_id = fa.ayat_id
                LEFT JOIN Pages p ON p.page_id = fa.page_id
            )
            UPDATE Suras
               SET page_number = (SELECT d.page_number FROM derived d WHERE d.sura_id = Suras.sura_id),
                   line_number = (SELECT d.line_number FROM derived d WHERE d.sura_id = Suras.sura_id)
            '''
        )
        conn.commit()

    cur.execute('SELECT COUNT(*) FROM Suras WHERE page_number IS NOT NULL')
    pages_filled = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM Suras WHERE line_number IS NOT NULL')
    lines_filled = cur.fetchone()[0]
    return pages_filled, lines_filled


def main() -> None:
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)
    pages, lines = update_suras_page_line(conn)

    # Show sample
    cur = conn.cursor()
    cur.execute('SELECT sura_id, name_arabic, page_number, line_number FROM Suras ORDER BY sura_id LIMIT 5')
    sample = cur.fetchall()
    conn.close()

    print(f"Updated Suras page_number for {pages} rows")
    print(f"Updated Suras line_number for {lines} rows")
    for r in sample:
        print(r)


if __name__ == '__main__':
    main()
