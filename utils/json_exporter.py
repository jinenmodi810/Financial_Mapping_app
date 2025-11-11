import json
from collections import defaultdict
from utils.db_handler import get_connection

def export_mappings_to_json(cik: str):
    """
    Export all mappings grouped by statement_type and library_term into JSON.
    {
      "income_statement": {...},
      "balance_sheet": {...},
      "cashflow_statement": {...},
      "equity_statement": {...}
    }
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT statement_type, library_term, us_gaap_tag
        FROM term_mappings
        WHERE cik = %s
          AND library_term IS NOT NULL
          AND TRIM(library_term) != ''
        ORDER BY statement_type, library_term
    """, (cik,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    output = {
        "income_statement": defaultdict(list),
        "balance_sheet": defaultdict(list),
        "cashflow_statement": defaultdict(list),
        "equity_statement": defaultdict(list)
    }

    for row in rows:
        stype = row["statement_type"].lower()
        lib = row["library_term"].strip()
        tag = f"us-gaap:{row['us_gaap_tag'].strip()}"
        if stype == "income":
            output["income_statement"][lib].append(tag)
        elif stype == "balance":
            output["balance_sheet"][lib].append(tag)
        elif stype == "cashflow":
            output["cashflow_statement"][lib].append(tag)
        elif stype == "equity":
            output["equity_statement"][lib].append(tag)

    final_output = {k: dict(v) for k, v in output.items()}
    return json.dumps(final_output, indent=2)