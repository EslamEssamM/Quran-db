import os
import time
import threading
import requests
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Connect to SQLite database (creates if not exists)
conn = sqlite3.connect('quran_arabic.db')
cursor = conn.cursor()

# Speed up bulk inserts
cursor.execute('PRAGMA journal_mode=WAL')
cursor.execute('PRAGMA synchronous=NORMAL')
cursor.execute('PRAGMA temp_store=MEMORY')
cursor.execute('PRAGMA cache_size=-100000')  # ~100MB cache

# Create tables (Arabic-only schema)
cursor.execute('''
CREATE TABLE IF NOT EXISTS Suras (
    sura_id INTEGER PRIMARY KEY,
    name_arabic TEXT NOT NULL,
    revelation_order INTEGER NOT NULL,
    ayat_count INTEGER NOT NULL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Ayats (
    ayat_id INTEGER PRIMARY KEY,
    sura_id INTEGER NOT NULL,
    ayat_number INTEGER NOT NULL,
    text_arabic TEXT NOT NULL,
    juz_id INTEGER NOT NULL,
    hezb_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    FOREIGN KEY (sura_id) REFERENCES Suras(sura_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Words (
    word_id INTEGER PRIMARY KEY,
    ayat_id INTEGER NOT NULL,
    word_number INTEGER NOT NULL,
    text_arabic TEXT NOT NULL,
    FOREIGN KEY (ayat_id) REFERENCES Ayats(ayat_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Juzs (
    juz_id INTEGER PRIMARY KEY,
    juz_number INTEGER NOT NULL UNIQUE
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Hezbs (
    hezb_id INTEGER PRIMARY KEY,
    hezb_number INTEGER NOT NULL UNIQUE,
    juz_id INTEGER NOT NULL,
    FOREIGN KEY (juz_id) REFERENCES Juzs(juz_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Pages (
    page_id INTEGER PRIMARY KEY,
    page_number INTEGER NOT NULL UNIQUE
)
''')

# Add indexes
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ayat_sura ON Ayats(sura_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_word_ayat ON Words(ayat_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ayat_juz ON Ayats(juz_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ayat_hezb ON Ayats(hezb_id)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_ayat_page ON Ayats(page_id)')

# Populate static tables: Juzs, Hezbs, Pages
for juz_num in range(1, 31):
    cursor.execute(
        'INSERT OR IGNORE INTO Juzs (juz_id, juz_number) VALUES (?, ?)', (juz_num, juz_num))

for hezb_num in range(1, 61):
    # Hezbs 1-2 in Juz 1, 3-4 in Juz 2, etc.
    juz_id = ((hezb_num - 1) // 2) + 1
    cursor.execute('INSERT OR IGNORE INTO Hezbs (hezb_id, hezb_number, juz_id) VALUES (?, ?, ?)',
                   (hezb_num, hezb_num, juz_id))

for page_num in range(1, 605):
    cursor.execute(
        'INSERT OR IGNORE INTO Pages (page_id, page_number) VALUES (?, ?)', (page_num, page_num))

# Fetch and populate Suras
suras_response = requests.get(
    'https://api.quran.com/api/v4/chapters?language=ar')
suras = suras_response.json()['chapters']
for sura in suras:
    cursor.execute('''
    INSERT OR IGNORE INTO Suras (sura_id, name_arabic, revelation_order, ayat_count)
    VALUES (?, ?, ?, ?)
    ''', (sura['id'], sura['name_arabic'], sura['revelation_order'], sura['verses_count']))

#############################
# Parallel fetching utilities
#############################

tls = threading.local()


def _make_session() -> requests.Session:
    s = requests.Session()
    # Retries with backoff for transient errors/rate limits
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    s.headers.update({
        'User-Agent': 'QuranFetcher/1.0 (+https://api.quran.com)'
    })
    return s


def _get_session() -> requests.Session:
    if getattr(tls, 'session', None) is None:
        tls.session = _make_session()
    return tls.session


def fetch_ayah(sura_id: int, ayat_number: int):
    """Fetch a single ayah with words and metadata. Returns tuple(key, payload) or raises."""
    session = _get_session()
    url = (
        f'https://api.quran.com/api/v4/verses/by_key/{sura_id}:{ayat_number}'
        '?words=true&word_fields=text_uthmani&fields=juz_number,hizb_number,page_number,text_uthmani'
    )
    resp = session.get(url, timeout=(10, 60))
    resp.raise_for_status()
    data = resp.json()['verse']
    # Trim down to fields we need to store
    payload = {
        'text_uthmani': data['text_uthmani'],
        'juz_number': data['juz_number'],
        'hizb_number': data['hizb_number'],
        'page_number': data['page_number'],
        'words': [{'position': w['position'], 'text_uthmani': w['text_uthmani']} for w in data['words']],
    }
    return (sura_id, ayat_number), payload


# Populate Ayats and Words using parallel HTTP fetch, then ordered DB insert
start_time = time.time()

# Build ordered list of all ayah keys
all_ayah_keys = []
for sura_id in range(1, 115):
    sura_data = next(s for s in suras if s['id'] == sura_id)
    for ayat_number in range(1, sura_data['verses_count'] + 1):
        all_ayah_keys.append((sura_id, ayat_number))

max_workers_env = os.getenv('QURAN_MAX_WORKERS')
if max_workers_env and max_workers_env.isdigit():
    max_workers = int(max_workers_env)
else:
    # Reasonable default: plenty of concurrency but not too aggressive
    cpu = os.cpu_count() or 4
    max_workers = min(32, cpu * 4)

print(
    f"Fetching {len(all_ayah_keys)} ayahs with up to {max_workers} parallel workers...")

results = {}
errors = []
progress_every = 250

with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='ayah') as exe:
    future_map = {exe.submit(fetch_ayah, sid, anum): (sid, anum)
                  for sid, anum in all_ayah_keys}
    completed = 0
    for fut in as_completed(future_map):
        key = future_map[fut]
        try:
            k, payload = fut.result()
            results[k] = payload
        except Exception as e:
            errors.append((key, str(e)))
        finally:
            completed += 1
            if completed % progress_every == 0:
                print(f"Fetched {completed}/{len(all_ayah_keys)} ayahs...")

if errors:
    print(
        f"Encountered {len(errors)} fetch errors; retrying once serially for failed items...")
    # One more gentle pass for failed ones
    for (sid, anum), _err in errors:
        try:
            k, payload = fetch_ayah(sid, anum)
            results[k] = payload
        except Exception as e:
            print(f"Final failure fetching {sid}:{anum} -> {e}")

print("HTTP fetching complete. Inserting into database...")

# Insert in deterministic order to preserve ayat_id and word_id continuity
ayat_id = 1
word_id = 1
for sura_id in range(1, 115):
    sura_data = next(s for s in suras if s['id'] == sura_id)
    for ayat_number in range(1, sura_data['verses_count'] + 1):
        payload = results.get((sura_id, ayat_number))
        if not payload:
            # Skip if still missing (unrecoverable failures); continue with consistent IDs by not incrementing
            print(f"Skipping missing {sura_id}:{ayat_number}")
            continue

        cursor.execute('''
        INSERT INTO Ayats (ayat_id, sura_id, ayat_number, text_arabic, juz_id, hezb_id, page_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            ayat_id,
            sura_id,
            ayat_number,
            payload['text_uthmani'],
            payload['juz_number'],
            payload['hizb_number'],
            payload['page_number'],
        ))

        # Insert words for this ayah
        for w in payload['words']:
            cursor.execute('''
            INSERT INTO Words (word_id, ayat_id, word_number, text_arabic)
            VALUES (?, ?, ?, ?)
            ''', (word_id, ayat_id, w['position'], w['text_uthmani']))
            word_id += 1

        ayat_id += 1

elapsed = time.time() - start_time
print(f"Inserted data for {ayat_id - 1} ayahs in {elapsed:.1f}s")

# Commit changes and close connection
conn.commit()
conn.close()

print("Database population complete. 'quran_arabic.db' created with Arabic-only data.")
