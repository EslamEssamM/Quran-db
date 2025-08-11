import os
import sqlite3
import zipfile
from datetime import datetime

DB_PATH = 'quran_arabic.db'
OPTIMIZED_DB = 'quran_arabic.optimized.db'
ZIP_PATH = 'quran_arabic.db.zip'


def human(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def compact_db(db_path: str = DB_PATH) -> str | None:
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}")
        return None

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Make sure WAL is checkpointed and truncated to minimize side files
    cur.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    # Run query planner and stats maintenance
    cur.execute('PRAGMA optimize')
    conn.commit()

    # Try to write an optimized copy without touching the original file
    try:
        cur.execute(f"VACUUM INTO '{OPTIMIZED_DB}'")
        conn.commit()
        print(f"Wrote optimized copy: {OPTIMIZED_DB}")
        return OPTIMIZED_DB
    except sqlite3.DatabaseError as e:
        print(f"VACUUM INTO unsupported ({e}); attempting in-place VACUUM...")
        try:
            cur.execute('VACUUM')
            conn.commit()
            print("In-place VACUUM completed.")
        finally:
            conn.close()
        # In-place, so return the same path
        return db_path
    finally:
        try:
            conn.close()
        except Exception:
            pass


def make_zip(file_to_zip: str, zip_path: str = ZIP_PATH) -> str:
    if not os.path.exists(file_to_zip):
        raise FileNotFoundError(file_to_zip)
    # Include a timestamped name inside the zip to avoid name collisions when extracted
    arcname = os.path.basename(file_to_zip)
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    arcname_ts = f"{os.path.splitext(arcname)[0]}-{ts}{os.path.splitext(arcname)[1]}"
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(file_to_zip, arcname=arcname_ts)
    return zip_path


def main() -> None:
    original_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    print(f"Original DB size: {human(original_size)}")

    optimized = compact_db(DB_PATH)
    if not optimized:
        return

    opt_size = os.path.getsize(optimized)
    print(f"Optimized file: {optimized} ({human(opt_size)})")

    # Choose the smaller file for zipping
    candidate = optimized if opt_size and opt_size < original_size else DB_PATH
    zip_file = make_zip(candidate)
    zip_size = os.path.getsize(zip_file)
    print(f"Created zip: {zip_file} ({human(zip_size)}) from {os.path.basename(candidate)}")

    # Tips for distribution
    wal = DB_PATH + '-wal'
    shm = DB_PATH + '-shm'
    if os.path.exists(wal) or os.path.exists(shm):
        print("Note: WAL/SHM side files exist. They are not needed for distribution; zip only the .db copy.")


if __name__ == '__main__':
    main()
