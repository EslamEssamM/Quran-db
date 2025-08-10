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

    # Add page_number column to Juzs and Hezbs if missing
    if not column_exists(conn, 'Juzs', 'page_number'):
        cur.execute('ALTER TABLE Juzs ADD COLUMN page_number INTEGER')
    if not column_exists(conn, 'Hezbs', 'page_number'):
        cur.execute('ALTER TABLE Hezbs ADD COLUMN page_number INTEGER')
    conn.commit()


def update_juz_pages(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    # Compute min page_id per juz_id from Ayats
    cur.execute('''
        WITH min_pages AS (
            SELECT juz_id, MIN(page_id) AS min_page
            FROM Ayats
            GROUP BY juz_id
        )
        UPDATE Juzs
           SET page_number = (
                SELECT min_page FROM min_pages WHERE min_pages.juz_id = Juzs.juz_id
           )
    ''')
    conn.commit()
    # Count populated rows
    cur.execute('SELECT COUNT(*) FROM Juzs WHERE page_number IS NOT NULL')
    return cur.fetchone()[0]


def update_hezb_pages(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute('''
        WITH min_pages AS (
            SELECT hezb_id, MIN(page_id) AS min_page
            FROM Ayats
            GROUP BY hezb_id
        )
        UPDATE Hezbs
           SET page_number = (
                SELECT min_page FROM min_pages WHERE min_pages.hezb_id = Hezbs.hezb_id
           )
    ''')
    conn.commit()
    cur.execute('SELECT COUNT(*) FROM Hezbs WHERE page_number IS NOT NULL')
    return cur.fetchone()[0]


def main() -> None:
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)

    juz_filled = update_juz_pages(conn)
    hezb_filled = update_hezb_pages(conn)

    # Show small sample
    cur = conn.cursor()
    cur.execute('SELECT juz_id, juz_number, page_number FROM Juzs ORDER BY juz_id LIMIT 5')
    juz_rows = cur.fetchall()
    cur.execute('SELECT hezb_id, hezb_number, page_number FROM Hezbs ORDER BY hezb_id LIMIT 5')
    hezb_rows = cur.fetchall()

    conn.close()

    print(f"Updated Juzs page_number for {juz_filled} rows")
    for r in juz_rows:
        print(r)
    print(f"Updated Hezbs page_number for {hezb_filled} rows")
    for r in hezb_rows:
        print(r)


if __name__ == '__main__':
    main()
