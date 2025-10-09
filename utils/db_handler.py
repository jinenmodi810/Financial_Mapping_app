import sqlite3
import os

DB_PATH = "data/mappings/company_mappings.db"

# -----------------------------------------------------
# ✅ Initialize database (now includes statement_type)
# -----------------------------------------------------
def init_db():
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
            library_term TEXT,
            UNIQUE(cik, us_gaap_tag, statement_type)
        )
    """)
    conn.commit()
    conn.close()


# -----------------------------------------------------
# ✅ Get all distinct library terms (for dropdown)
# -----------------------------------------------------
def get_all_library_terms():
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


# -----------------------------------------------------
# ✅ Save mappings (overwrite existing entries safely)
# -----------------------------------------------------
def save_mappings_with_type(cik: str, company_name: str, statement_type: str, mappings):
    """
    Save or overwrite mappings for a given statement type.
    Prevents duplicate entries and updates existing ones.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for tag, lib in mappings:
        cur.execute("""
            INSERT INTO term_mappings (cik, company_name, statement_type, us_gaap_tag, library_term)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, us_gaap_tag, statement_type)
            DO UPDATE SET
                library_term = excluded.library_term,
                company_name = excluded.company_name
        """, (cik, company_name, statement_type, tag, lib))

    conn.commit()
    conn.close()


# -----------------------------------------------------
# ✅ Legacy support (backward compatibility)
# -----------------------------------------------------
def save_mappings(cik: str, company_name: str, mappings):
    """
    Fallback for scripts not using statement_type.
    Default statement_type='unknown'.
    """
    save_mappings_with_type(cik, company_name, "unknown", mappings)


# -----------------------------------------------------
# ✅ Get company-wise progress summary
# -----------------------------------------------------
def get_company_progress_summary():
    """
    Returns a summary list:
    [
      {'company_name': 'Expedia', 'cik': '1324424',
       'income': 15, 'balance': 10, 'cashflow': 8, 'equity': 0, 'total': 33},
       ...
    ]
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
    for name, cik, stmt, count in rows:
        stmt = (stmt or "unknown").lower()
        entry = summary.setdefault(
            (name, cik),
            {"income": 0, "balance": 0, "cashflow": 0, "equity": 0, "unknown": 0}
        )
        entry[stmt] = count

    result = []
    for (name, cik), counts in summary.items():
        counts["company_name"] = name
        counts["cik"] = cik
        counts["total"] = sum(counts.values())
        result.append(counts)

    return sorted(result, key=lambda x: x["total"], reverse=True)


# -----------------------------------------------------
# ✅ Get per-company, per-statement progress
# -----------------------------------------------------
def get_progress_for_company(cik: str):
    """
    Returns dict: { 'income': n, 'balance': n, 'cashflow': n, 'equity': n, 'total': n }
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

    progress = {"income": 0, "balance": 0, "cashflow": 0, "equity": 0}
    for stmt, count in rows:
        if stmt:
            progress[stmt.lower()] = count
    progress["total"] = sum(progress.values())
    return progress