# Quran-db (SQLite)

A small toolkit to build and maintain a local, queryable SQLite database of the Qur'an (Arabic – Uthmani script, 15-line mushaf layout), with Juz/Hizb/Page mappings and hooks for audio URLs and word-level segmentation.

The repo ships with helper scripts and seed metadata to assemble a single `quran_arabic.db` you can use in apps, research, or offline browsing.

## Resources and attribution

- Text and metadata sourced from: QUL, Corbus, Quran.com (<https://quran.com/>), Tanzil (<https://tanzil.net/>)
- Audio base used by scripts: <https://verses.quran.foundation/>
- Collected and consolidated by: *Eslam Essam Mohamed*
- Usage: Free to use or distribute — لا تنسونا من صالح دعائكم

## Highlights

- SQLite schema for Surahs, Ayahs, Words, Juzs, Hizbs, and Pages
- Page/Juz/Hizb mapping (15-line mushaf layout)
- Word-level rows with basic metadata (type/page/line) and optional per-word audio URL
- Resilient download helper with retries (HTTP 429/5xx)
- Ready-to-use metadata sources under `data/`

## Repository layout

```text
.
├─ data/                          # Seed metadata databases
│  ├─ quran-metadata-ayah.sqlite
│  ├─ quran-metadata-juz.sqlite
│  ├─ quran-metadata-surah-name.sqlite
│  └─ uthmani-15-lines.db
├─ examples/
│  └─ data.json                   # Example shape/sample
├─ scribts/                       # Legacy/utility scripts (kept for reference)
│  ├─ download.py
│  ├─ download_v2.py
│  ├─ import_juzs.py
│  ├─ import_lines.py
│  └─ update_pages_for_juz_hezb.py
├─ download_v2.py                 # Current WIP downloader/refactor (see notes)
├─ import_juzs.py                 # Enrich Juzs table with verse ranges
├─ update_pages_for_juz_hezb.py   # Fill page_number for Juzs/Hezbs from Ayats
├─ quran_arabic.db                # Your working database (created/updated locally)
└─ README.md
```

Note: `download_v2.py` at the repo root is a refactor-in-progress. If you run into issues, use the corresponding script under `scribts/` as a fallback; the interfaces are similar.

## Requirements

- Python 3.10+
- Windows, macOS, or Linux
- Internet connection (for first-time downloads)

Install Python dependencies:

```cmd
pip install -r requirements.txt
```

This project primarily uses:

- requests (with urllib3 Retry) for HTTP
- sqlite3 from the Python standard library

## Quick start

1. Create/activate a virtual environment (optional but recommended):

```cmd
python -m venv .venv
.venv\Scripts\activate
```

1. Install dependencies:

```cmd
pip install -r requirements.txt
```

1. Download and build/refresh the database content:

- Preferred (current refactor):

```cmd
python download_v2.py
```

- Fallback (legacy script version):

```cmd
python scribts\download_v2.py
```

This will populate or update `quran_arabic.db` in the repository root.

1. Enrich Juz metadata (count and first/last ayah IDs):

```cmd
python import_juzs.py
```

This reads from `data\quran-metadata-juz.sqlite` and updates the `Juzs` table with `verses_count`, `first_ayat_id`, and `last_ayat_id`.

1. Derive first page per Juz/Hizb from actual Ayat placements:

```cmd
python update_pages_for_juz_hezb.py
```

This fills `page_number` in `Juzs` and `Hezbs` using the minimum page for each grouping.

1. Populate Surah page and line numbers (based on the first ayah of each surah):

```cmd
python update_suras_page_line.py
```

This adds/updates `Suras.page_number` and `Suras.line_number`.

## Packaging and compression

To compact the database and produce a compressed ZIP for sharing/releases:

```cmd
python compress_db.py
```

This will:

- Run PRAGMA wal_checkpoint + optimize
- Create an optimized copy via `VACUUM INTO` when supported (fallback to in-place VACUUM)
- Create `quran_arabic.db.zip` at maximum compression

## Database schema (summary)

Core tables created/used by the scripts:

- Suras
  - sura_id (PK), name_arabic, revelation_order, ayat_count
- Juzs
  - juz_id (PK), juz_number (unique)
  - verses_count, first_ayat_id, last_ayat_id (added by `import_juzs.py`)
  - page_number (added by `update_pages_for_juz_hezb.py`)
- Hezbs
  - hezb_id (PK), hezb_number (unique), juz_id (FK → Juzs)
  - page_number (added by `update_pages_for_juz_hezb.py`)
- Pages
  - page_id (PK), page_number (unique)
- Ayats
  - ayat_id (PK), sura_id (FK → Suras), ayat_number
  - text_uthmani
  - juz_id (FK → Juzs), hezb_id (FK → Hezbs), page_id (FK → Pages)
  - sajdah_number (nullable)
  - audio_url (nullable), audio_segments (nullable JSON/text)
- Words
  - word_id (PK), ayat_id (FK → Ayats), word_number
  - text_uthmani, type (char_type), page_number (nullable), line_number (nullable)
  - audio_url (nullable)

Indexes (non-exhaustive):

- `Ayats(sura_id, ayat_number)` for fast mapping (used by `import_juzs.py`).

You can explore the DB with your favorite SQLite client (e.g., DB Browser for SQLite) or the CLI (if `sqlite3` is installed):

```cmd
sqlite3 quran_arabic.db ".tables"
sqlite3 quran_arabic.db "SELECT sura_id, name_arabic, ayat_count FROM Suras LIMIT 5;"
```

Please review the terms of use of the upstream sources and attribute accordingly in your applications.

## Troubleshooting

- ModuleNotFoundError: requests
  - Run `pip install -r requirements.txt` in your active virtual environment.
- Database is locked / WAL files present
  - Make sure no other process is using `quran_arabic.db`. The scripts enable WAL mode for performance; close DB viewers before writing.
- Incomplete data after download
  - Re-run the downloader and ensure network connectivity. If the root `download_v2.py` fails, try the version under `scribts/`.

## Contributing

- Issues and PRs are welcome. Please include a clear description and, when possible, sample data/queries.
- Keep changes minimal and schema-safe; avoid breaking existing table/column names unless coordinated.

## License

This repository consolidates public metadata and code. If you plan to distribute a modified DB, please verify licenses/terms for any upstream content bundled in your distribution.
