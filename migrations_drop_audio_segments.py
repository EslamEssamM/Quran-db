import os
import sqlite3

DB_PATH = 'quran_arabic.db'

DDL_NEW_AYATS = '''
CREATE TABLE Ayats_new (
    ayat_id INTEGER PRIMARY KEY,
    sura_id INTEGER NOT NULL,
    ayat_number INTEGER NOT NULL,
    text_uthmani TEXT NOT NULL,
    juz_id INTEGER NOT NULL,
    hezb_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    sajdah_number INTEGER,
    audio_url TEXT,
    FOREIGN KEY (sura_id) REFERENCES Suras(sura_id),
    FOREIGN KEY (juz_id) REFERENCES Juzs(juz_id),
    FOREIGN KEY (hezb_id) REFERENCES Hezbs(hezb_id),
    FOREIGN KEY (page_id) REFERENCES Pages(page_id)
)'''


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def drop_audio_segments() -> None:
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys=OFF')
    conn.commit()

    try:
        if not column_exists(conn, 'Ayats', 'audio_segments'):
            print('Column audio_segments does not exist; nothing to do.')
            return

        # Try ALTER TABLE DROP COLUMN (supported in SQLite 3.35+)
        try:
            cur.execute('ALTER TABLE Ayats DROP COLUMN audio_segments')
            conn.commit()
            print('Dropped column audio_segments via ALTER TABLE.')
            return
        except sqlite3.DatabaseError as e:
            print(f'ALTER TABLE DROP COLUMN not supported, rebuilding table... ({e})')

        # Fallback: rebuild table
        cur.execute(DDL_NEW_AYATS)
        cur.execute('''
            INSERT INTO Ayats_new (
                ayat_id, sura_id, ayat_number, text_uthmani, juz_id, hezb_id, page_id, sajdah_number, audio_url
            )
            SELECT ayat_id, sura_id, ayat_number, text_uthmani, juz_id, hezb_id, page_id, sajdah_number, audio_url
            FROM Ayats
        ''')
        cur.execute('DROP TABLE Ayats')
        cur.execute('ALTER TABLE Ayats_new RENAME TO Ayats')
        conn.commit()
        print('Rebuilt Ayats without audio_segments.')
    finally:
        cur.execute('PRAGMA foreign_keys=ON')
        conn.commit()
        conn.close()


if __name__ == '__main__':
    drop_audio_segments()
