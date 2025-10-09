import sqlite3
import os

DB_PATH = "data/mappings/company_mappings.db"

def init_db():
    """Initialize SQLite database and ensure schema + index exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS term_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT,
            company_name TEXT,
            statement_type TEXT,
            us_gaap_tag TEXT,
            library_term TEXT
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_mapping
        ON term_mappings (cik, statement_type, us_gaap_tag);
    """)
    conn.commit()
    conn.close()
    print(f"[DB] ✅ Schema ready at {DB_PATH}")


def save_mappings_with_type(cik: str, company_name: str, statement_type: str, mappings):
    """Save or update mappings for a statement type."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for tag, lib in mappings:
        cur.execute("""
            INSERT INTO term_mappings (cik, company_name, statement_type, us_gaap_tag, library_term)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, statement_type, us_gaap_tag)
            DO UPDATE SET library_term = excluded.library_term
        """, (cik, company_name, statement_type, tag, lib))
    conn.commit()
    conn.close()


def get_all_library_terms():
    """Return distinct library terms."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT library_term 
        FROM term_mappings 
        WHERE library_term IS NOT NULL AND TRIM(library_term) != ''
    """)
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return sorted(rows)


def get_company_progress_summary():
    """Return progress per company and statement type."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT company_name, cik, statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        GROUP BY company_name, cik, statement_type
    """)
    rows = cur.fetchall()
    conn.close()

    summary = {}
    for name, cik, stype, count in rows:
        summary.setdefault((name, cik), {})[stype] = count
    return summary


def get_progress_for_company(cik: str):
    """Return progress for one company."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        WHERE cik = ?
        GROUP BY statement_type
    """, (cik,))
    rows = cur.fetchall()
    conn.close()
    return {stype: count for stype, count in rows}

def get_saved_mappings(cik: str, statement_type: str):
    """
    Fetch all previously saved mappings for a company + statement type.
    Returns a dict: {us_gaap_tag: library_term}
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT us_gaap_tag, library_term
        FROM term_mappings
        WHERE cik = ? AND statement_type = ?
    """, (cik, statement_type))
    rows = cur.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}