import sqlite3
import os

DB_PATH = "data/mappings/company_mappings.db"

def init_db():
    """
    Initialize the SQLite database and ensure the schema is up-to-date.
    Automatically adds missing columns if upgrading from older versions.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create table if not exists
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

    # Ensure required columns exist (safe migration)
    existing_cols = [r[1] for r in cur.execute("PRAGMA table_info(term_mappings)").fetchall()]
    if "statement_type" not in existing_cols:
        cur.execute("ALTER TABLE term_mappings ADD COLUMN statement_type TEXT;")
        conn.commit()

    conn.commit()
    conn.close()


def get_all_library_terms():
    """Return list of all distinct library terms stored so far."""
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


def save_mappings(cik: str, company_name: str, mappings):
    """Legacy save (without statement_type)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for tag, lib in mappings:
        cur.execute(
            "INSERT INTO term_mappings (cik, company_name, us_gaap_tag, library_term) VALUES (?, ?, ?, ?)",
            (cik, company_name, tag, lib)
        )
    conn.commit()
    conn.close()


def save_mappings_with_type(cik: str, company_name: str, statement_type: str, mappings):
    """
    Save mappings with statement type (income/balance/cashflow/equity).
    If a mapping already exists for (cik, statement_type, us_gaap_tag), it overwrites it.
    """
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


def get_company_progress_summary():
    """
    Return overall mapping progress per company and statement type.
    Example output:
        {('Apple Inc.', '0000320193'): {'income': 25, 'balance': 30}}
    """
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
    """
    Return mapping progress for a specific company (CIC).
    Example output:
        {'income': 15, 'balance': 10, 'cashflow': 8}
    """
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