import os
import sqlite3
from typing import List, Tuple

TARGET_DB = 'quran_arabic.db'
SOURCE_DB = 'uthmani-15-lines.db'


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Performance / integrity pragmas
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA foreign_keys=ON')

    # Lines table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Lines (
        line_id INTEGER PRIMARY KEY,
        page_id INTEGER NOT NULL,
        line_number INTEGER NOT NULL,
        line_type TEXT,
        is_centered INTEGER NOT NULL,
        first_word_id INTEGER,
        last_word_id INTEGER,
        sura_id INTEGER,
        FOREIGN KEY (page_id) REFERENCES Pages(page_id),
        FOREIGN KEY (sura_id) REFERENCES Suras(sura_id)
    )
    ''')
    # Indexes / constraints
    cur.execute('CREATE INDEX IF NOT EXISTS idx_lines_page ON Lines(page_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_lines_sura ON Lines(sura_id)')
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_lines_page_line ON Lines(page_id, line_number)')
    conn.commit()


def load_source_rows(src_conn: sqlite3.Connection) -> List[Tuple[int, int, str, int, int, int, int]]:
    cur = src_conn.cursor()
    cur.execute('''
        SELECT page_number, line_number, line_type, is_centered, first_word_id, last_word_id, surah_number
        FROM pages
        ORDER BY page_number, line_number
    ''')
    return cur.fetchall()


def get_valid_ids(conn: sqlite3.Connection) -> Tuple[set, set]:
    cur = conn.cursor()
    cur.execute('SELECT page_id FROM Pages')
    valid_pages = {row[0] for row in cur.fetchall()}
    cur.execute('SELECT sura_id FROM Suras')
    valid_suras = {row[0] for row in cur.fetchall()}
    return valid_pages, valid_suras


def import_lines() -> None:
    if not os.path.exists(SOURCE_DB):
        print(f"Source DB not found: {SOURCE_DB}")
        return
    if not os.path.exists(TARGET_DB):
        print(f"Target DB not found: {TARGET_DB}")
        return

    tgt = sqlite3.connect(TARGET_DB)
    # delete existing lines
    tgt.execute('DELETE FROM Lines')
    # delete the lines table if exists
    tgt.execute('DROP TABLE IF EXISTS Lines')
    
    ensure_schema(tgt)

    src = sqlite3.connect('file:' + SOURCE_DB + '?mode=ro', uri=True)

    rows = load_source_rows(src)
    print(f"Loaded {len(rows)} lines from source")

    valid_pages, valid_suras = get_valid_ids(tgt)
    missing_fk = 0

    to_insert = []
    for page_number, line_number, line_type, is_centered, first_word_id, last_word_id, surah_number in rows:
        page_id = page_number
        sura_id = surah_number
        if page_id not in valid_pages:
            missing_fk += 1
            continue
        to_insert.append((
            page_id,
            line_number,
            line_type,
            1 if int(is_centered or 0) != 0 else 0,
            first_word_id,
            last_word_id,
            sura_id,
        ))

    cur = tgt.cursor()
    cur.executemany('''INSERT OR IGNORE INTO Lines (page_id, line_number, line_type, is_centered, first_word_id, last_word_id, sura_id) VALUES (?, ?, ?, ?, ?, ?, ?)''', to_insert)
    tgt.commit()

    print(f"Inserted {cur.rowcount if hasattr(cur, 'rowcount') else len(to_insert)} lines (attempted). Skipped {missing_fk} due to missing FKs.")

    # Show totals
    cur.execute('SELECT COUNT(*) FROM Lines')
    total = cur.fetchone()[0]
    print(f"Lines total now: {total}")

    src.close()
    tgt.close()


if __name__ == '__main__':
    import_lines()
