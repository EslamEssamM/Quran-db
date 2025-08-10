import os
import sqlite3
from typing import Tuple, Set

TARGET_DB = 'quran_arabic.db'
SOURCE_DB = 'quran-metadata-juz.sqlite'


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    return column in cols


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Performance and integrity
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=NORMAL')
    cur.execute('PRAGMA foreign_keys=ON')

    # Ensure columns on Juzs
    if not column_exists(conn, 'Juzs', 'verses_count'):
        cur.execute('ALTER TABLE Juzs ADD COLUMN verses_count INTEGER')
    if not column_exists(conn, 'Juzs', 'first_ayat_id'):
        cur.execute('ALTER TABLE Juzs ADD COLUMN first_ayat_id INTEGER')
    if not column_exists(conn, 'Juzs', 'last_ayat_id'):
        cur.execute('ALTER TABLE Juzs ADD COLUMN last_ayat_id INTEGER')

    # Helpful index for mapping (sura_id, ayat_number) -> ayat_id
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ayats_sura_number ON Ayats(sura_id, ayat_number)')

    conn.commit()


def get_valid_ayats(conn: sqlite3.Connection) -> Set[Tuple[int, int]]:
    # Not necessary to load all; we'll query per verse. Keep function in case of future optimization.
    return set()


def parse_verse_key(key: str) -> Tuple[int, int]:
    # format like '2:142'
    parts = key.split(':')
    if len(parts) != 2:
        raise ValueError(f'Invalid verse key: {key}')
    return int(parts[0]), int(parts[1])


def map_to_ayat_id(conn: sqlite3.Connection, sura_id: int, ayat_number: int) -> int:
    cur = conn.cursor()
    cur.execute('SELECT ayat_id FROM Ayats WHERE sura_id=? AND ayat_number=? LIMIT 1', (sura_id, ayat_number))
    row = cur.fetchone()
    if not row:
        raise LookupError(f'Ayat not found for {sura_id}:{ayat_number}')
    return row[0]


def import_juzs() -> None:
    if not os.path.exists(SOURCE_DB):
        print(f"Source DB not found: {SOURCE_DB}")
        return
    if not os.path.exists(TARGET_DB):
        print(f"Target DB not found: {TARGET_DB}")
        return

    tgt = sqlite3.connect(TARGET_DB)
    ensure_schema(tgt)

    src = sqlite3.connect('file:' + SOURCE_DB + '?mode=ro', uri=True)
    scur = src.cursor()

    scur.execute('SELECT juz_number, verses_count, first_verse_key, last_verse_key FROM juz ORDER BY juz_number')

    tcur = tgt.cursor()
    updated = 0
    skipped = 0

    for juz_number, verses_count, first_key, last_key in scur.fetchall():
        try:
            f_sura, f_ayah = parse_verse_key(first_key)
            l_sura, l_ayah = parse_verse_key(last_key)
            first_id = map_to_ayat_id(tgt, f_sura, f_ayah)
            last_id = map_to_ayat_id(tgt, l_sura, l_ayah)

            # Update by juz_id (which equals juz_number in this DB)
            tcur.execute('''
                UPDATE Juzs
                   SET verses_count = ?, first_ayat_id = ?, last_ayat_id = ?
                 WHERE juz_id = ?
            ''', (verses_count, first_id, last_id, juz_number))
            updated += 1
        except Exception as e:
            print(f"Skip Juz {juz_number}: {e}")
            skipped += 1

    tgt.commit()

    print(f"Juzs updated: {updated}, skipped: {skipped}")

    # Show a small sample
    tcur.execute('SELECT juz_id, juz_number, verses_count, first_ayat_id, last_ayat_id FROM Juzs ORDER BY juz_id LIMIT 5')
    for row in tcur.fetchall():
        print(row)

    src.close()
    tgt.close()


if __name__ == '__main__':
    import_juzs()
