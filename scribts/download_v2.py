import os
import json
import time
import threading
import requests
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_AUDIO = 'https://verses.quran.foundation/'

DB_PATH = 'quran_arabic.db'


def combine_url(path: str | None) -> str | None:
    if not path:
        return None
    # Avoid double slashes
    return BASE_AUDIO.rstrip('/') + '/' + path.lstrip('/')


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    s.headers.update({'User-Agent': 'QuranFetcher/2.0 (+https://api.quran.com)'})
    return s


def ensure_base_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=NORMAL')
    cur.execute('PRAGMA temp_store=MEMORY')
    cur.execute('PRAGMA cache_size=-100000')

    # Base tables if missing
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Suras (
        sura_id INTEGER PRIMARY KEY,
        name_arabic TEXT NOT NULL,
        revelation_order INTEGER NOT NULL,
        ayat_count INTEGER NOT NULL
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Juzs (
        juz_id INTEGER PRIMARY KEY,
        juz_number INTEGER NOT NULL UNIQUE
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Hezbs (
        hezb_id INTEGER PRIMARY KEY,
        hezb_number INTEGER NOT NULL UNIQUE,
        juz_id INTEGER NOT NULL,
        FOREIGN KEY (juz_id) REFERENCES Juzs(juz_id)
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS Pages (
        page_id INTEGER PRIMARY KEY,
        page_number INTEGER NOT NULL UNIQUE
    )''')
    conn.commit()


def recreate_ayats_words(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Drop existing to match new schema
    cur.execute('DROP TABLE IF EXISTS Words')
    cur.execute('DROP TABLE IF EXISTS Ayats')

    # New Ayats schema
    cur.execute('''
    CREATE TABLE Ayats (
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
    )''')

    # New Words schema
    cur.execute('''
    CREATE TABLE Words (
        word_id INTEGER PRIMARY KEY,
        ayat_id INTEGER NOT NULL,
        word_number INTEGER NOT NULL,
        text_uthmani TEXT NOT NULL,
        type TEXT NOT NULL,
        page_number INTEGER,
        line_number INTEGER,
        audio_url TEXT,
        FOREIGN KEY (ayat_id) REFERENCES Ayats(ayat_id)
    )''')

    # Indexes
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ayat_sura ON Ayats(sura_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ayat_juz ON Ayats(juz_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ayat_hezb ON Ayats(hezb_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ayat_page ON Ayats(page_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_word_ayat ON Words(ayat_id)')
    conn.commit()


tl = threading.local()


def get_session() -> requests.Session:
    if not getattr(tl, 'session', None):
        tl.session = make_session()
    return tl.session


API_BASE = 'https://api.quran.com/api/v4/verses/by_key/'
# Include needed fields for schema
FIELDS = 'text_uthmani,chapter_id,page_number,juz_number,hizb_number,sajdah_number'
WORD_FIELDS = 'text_uthmani,page_number,line_number,char_type,audio'


def fetch_verse(sura_id: int, ayah_number: int):
    session = get_session()
    url = (
        f"{API_BASE}{sura_id}:{ayah_number}?words=true&audio=7"
        f"&word_fields={WORD_FIELDS}"
        f"&fields={FIELDS}"
    )
    resp = session.get(url, timeout=(10, 60))
    resp.raise_for_status()
    verse = resp.json()['verse']
    # Normalize payload to our schema
    audio_url = None
    if verse.get('audio'):
        audio_url = combine_url(verse['audio'].get('url'))

    words = []
    for w in verse.get('words', []):
        words.append({
            'position': w.get('position'),
            'text_uthmani': w.get('text_uthmani'),
            'type': w.get('char_type_name') or w.get('char_type') or '',
            'page_number': w.get('page_number'),
            'line_number': w.get('line_number'),
            'audio_url': combine_url(w.get('audio_url') or (w.get('audio') or {}).get('url')),
        })

    payload = {
        'sura_id': verse.get('chapter_id'),
        'ayat_number': verse.get('verse_number') or ayah_number,
        'text_uthmani': verse.get('text_uthmani') or '',
        'juz_id': verse.get('juz_number'),
        'hezb_id': verse.get('hizb_number') or verse.get('rub_el_hizb_number'),
        'page_id': verse.get('page_number'),
        'sajdah_number': verse.get('sajdah_number'),
    'audio_url': audio_url,
        'words': words,
    }
    return (sura_id, ayah_number), payload


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    ensure_base_tables(conn)

    # Ensure static tables are populated
    cur = conn.cursor()
    for juz_num in range(1, 31):
        cur.execute('INSERT OR IGNORE INTO Juzs (juz_id, juz_number) VALUES (?, ?)', (juz_num, juz_num))
    for hezb_num in range(1, 61):
        juz_id = ((hezb_num - 1) // 2) + 1
        cur.execute('INSERT OR IGNORE INTO Hezbs (hezb_id, hezb_number, juz_id) VALUES (?, ?, ?)', (hezb_num, hezb_num, juz_id))
    for page_num in range(1, 605):
        cur.execute('INSERT OR IGNORE INTO Pages (page_id, page_number) VALUES (?, ?)', (page_num, page_num))
    conn.commit()

    recreate_ayats_words(conn)

    # Get sura metadata for counts
    s = make_session()
    suras_resp = s.get('https://api.quran.com/api/v4/chapters?language=ar', timeout=(10, 30))
    suras_resp.raise_for_status()
    suras = suras_resp.json()['chapters']
    # Populate Suras table if needed
    for sura in suras:
        cur.execute('''
            INSERT OR IGNORE INTO Suras (sura_id, name_arabic, revelation_order, ayat_count)
            VALUES (?, ?, ?, ?)
        ''', (sura['id'], sura['name_arabic'], sura['revelation_order'], sura['verses_count']))
    conn.commit()

    # Build list of all ayah keys
    all_ayah = []
    for sura in suras:
        sid = sura['id']
        for anum in range(1, sura['verses_count'] + 1):
            all_ayah.append((sid, anum))

    print(f"Fetching {len(all_ayah)} verses with audio and word metadata...")

    max_workers_env = os.getenv('QURAN_MAX_WORKERS')
    if max_workers_env and max_workers_env.isdigit():
        max_workers = int(max_workers_env)
    else:
        cpu = os.cpu_count() or 4
        max_workers = min(32, cpu * 4)

    results: dict[tuple[int, int], dict] = {}
    errors: list[tuple[tuple[int, int], str]] = []

    start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = {exe.submit(fetch_verse, sid, anum): (sid, anum) for sid, anum in all_ayah}
        completed = 0
        for fut in as_completed(futs):
            key = futs[fut]
            try:
                k, payload = fut.result()
                results[k] = payload
            except Exception as e:
                errors.append((key, str(e)))
            completed += 1
            if completed % 200 == 0:
                print(f"Fetched {completed}/{len(all_ayah)}")

    if errors:
        print(f"{len(errors)} errors during fetch; retrying serially...")
        for key, _ in errors:
            sid, anum = key
            try:
                k, payload = fetch_verse(sid, anum)
                results[k] = payload
            except Exception as e:
                print(f"Failed final fetch {sid}:{anum} -> {e}")

    print("Inserting into database...")

    # Insert in deterministic order
    ayat_id = 1
    word_id = 1
    for sura in suras:
        sid = sura['id']
        for anum in range(1, sura['verses_count'] + 1):
            payload = results.get((sid, anum))
            if not payload:
                print(f"Missing {sid}:{anum}")
                continue
            cur.execute('''
                INSERT INTO Ayats (
                    ayat_id, sura_id, ayat_number, text_uthmani, juz_id, hezb_id, page_id, sajdah_number, audio_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ayat_id,
                payload['sura_id'],
                payload['ayat_number'],
                payload['text_uthmani'],
                payload['juz_id'],
                payload['hezb_id'],
                payload['page_id'],
                payload['sajdah_number'],
                payload['audio_url'],
            ))
            for w in payload['words']:
                cur.execute('''
                    INSERT INTO Words (
                        word_id, ayat_id, word_number, text_uthmani, type, page_number, line_number, audio_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    word_id,
                    ayat_id,
                    w['position'],
                    w['text_uthmani'] or '',
                    w['type'] or '',
                    w['page_number'],
                    w['line_number'],
                    w['audio_url'],
                ))
                word_id += 1
            ayat_id += 1
    conn.commit()

    print(f"Done. Inserted {ayat_id - 1} ayats and {word_id - 1} words in {time.time() - start:.1f}s")

    conn.close()


if __name__ == '__main__':
    main()
