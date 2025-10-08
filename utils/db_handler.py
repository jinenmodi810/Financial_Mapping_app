import sqlite3, os

DB_PATH = "data/mappings/company_mappings.db"

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS term_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT,
            company_name TEXT,
            us_gaap_tag TEXT,
            library_term TEXT
        )
    """)
    conn.commit()
    conn.close()
def get_all_library_terms():
    """Return list of all distinct library terms stored so far."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT library_term FROM term_mappings WHERE library_term IS NOT NULL AND TRIM(library_term) != ''")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return sorted(rows)

def save_mappings(cik: str, company_name: str, mappings):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for tag, lib in mappings:
        cur.execute(
            "INSERT INTO term_mappings (cik, company_name, us_gaap_tag, library_term) VALUES (?, ?, ?, ?)",
            (cik, company_name, tag, lib)
        )
    conn.commit()
    conn.close()