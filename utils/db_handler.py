import mysql.connector
import os

DB_CONFIG = {
    "user": "ayoub",
    "password": "Password@2025",
    "host": "fintasenseai.mysql.database.azure.com",
    "port": 3306,
    "database": "fsfinancialmapper"
}


def get_connection(): 
    """Create a new MySQL connection."""
    conn = mysql.connector.connect(**DB_CONFIG)

    # ✅ Optional: safe short session timeout (prevents hanging sessions)
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION wait_timeout = 30;")
        cur.execute("SET SESSION interactive_timeout = 30;")
        cur.close()
    except Exception as e:
        print(f"[WARN] Could not set session timeouts: {e}")

    return conn


def init_db():
    """Initialize MySQL database and ensure schema + index exist."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS term_mappings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cik VARCHAR(50),
            company_name VARCHAR(255),
            statement_type VARCHAR(50),
            us_gaap_tag VARCHAR(255),
            library_term VARCHAR(255),
            UNIQUE KEY unique_mapping (cik, statement_type, us_gaap_tag)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] ✅ Schema ready in Azure MySQL.")


def save_mappings_with_type(cik: str, company_name: str, statement_type: str, mappings):
    """Save or update mappings for a statement type."""
    conn = get_connection()
    cur = conn.cursor()
    for tag, lib in mappings:
        cur.execute("""
            INSERT INTO term_mappings (cik, company_name, statement_type, us_gaap_tag, library_term)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE library_term = VALUES(library_term)
        """, (cik, company_name, statement_type, tag, lib))
    conn.commit()
    cur.close()
    conn.close()


def get_all_library_terms():
    """Return distinct library terms."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT library_term
        FROM term_mappings
        WHERE library_term IS NOT NULL AND TRIM(library_term) != ''
    """)
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return sorted(rows)


def get_company_progress_summary():
    """Return progress per company and statement type."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT company_name, cik, statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        GROUP BY company_name, cik, statement_type
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    summary = {}
    for name, cik, stype, count in rows:
        summary.setdefault((name, cik), {})[stype] = count
    return summary


def get_progress_for_company(cik: str):
    """Return progress for one company."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        WHERE cik = %s
        GROUP BY statement_type
    """, (cik,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {stype: count for stype, count in rows}


def get_saved_mappings(cik: str, statement_type: str):
    """Fetch all saved mappings for company + statement type."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT us_gaap_tag, library_term
        FROM term_mappings
        WHERE cik = %s AND statement_type = %s
    """, (cik, statement_type))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0]: r[1] for r in rows}


# =====================================================
# 🔹 Global Auto-Mapping Helper (now using MySQL)
# =====================================================

def get_global_mapping_dict():
    """
    Build a global memory of all GAAP→LibraryTerm mappings 
    across every company in the database.
    Returns dict: {us_gaap_tag: library_term}
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT us_gaap_tag, library_term
        FROM term_mappings
        WHERE library_term IS NOT NULL 
              AND TRIM(library_term) != ''
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0]: r[1] for r in rows}