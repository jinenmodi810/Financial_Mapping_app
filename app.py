import os
import json
import sqlite3
import pandas as pd
import streamlit as st

from utils.xbrl_statement_parser import build_statement_table
from utils.db_handler import init_db, get_all_library_terms

# Optional imports if present in your db_handler
try:
    from utils.db_handler import (
        save_mappings_with_type,
        get_company_progress_summary,
        get_progress_for_company,
    )
    HAVE_TYPED_SAVE = True
except Exception:
    from utils.db_handler import save_mappings  # fallback
    HAVE_TYPED_SAVE = False
    get_company_progress_summary = None
    get_progress_for_company = None

DB_PATH = "data/mappings/company_mappings.db"

st.set_page_config(page_title="Financial Term Mapper", layout="wide")

def inject_mobile_styles():
    st.markdown("""
    <style>
    .block-container {padding: 1rem !important;}
    div[data-baseweb="select"] > div {min-height: 48px !important;}
    button[kind="primary"] {min-height: 48px !important; font-size: 16px !important;}
    @media (max-width: 768px) {
        .stMarkdown, .stText, .stSelectbox, .stButton > button {
            font-size: 15px !important;
            line-height: 1.4em;
        }
        table {font-size: 13px !important;}
    }
    [data-testid="stDataFrameContainer"] {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch;
    }
    #MainMenu, footer, header {visibility: hidden;}
    .progress-card {
        padding: 10px 14px;
        margin-bottom: 10px;
        border-radius: 10px;
        background: #f8f9fa;
        border: 1px solid #ddd;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .badge {display:inline-block;padding:2px 8px;border-radius:8px;color:white;font-size:13px;margin-right:6px;}
    .income{background:#4caf50;}.balance{background:#2196f3;}
    .cashflow{background:#ff9800;}.equity{background:#9c27b0;}
    .total{background:#212121;}
    </style>
    """, unsafe_allow_html=True)

inject_mobile_styles()
init_db()

st.title("Financial Term Mapper")
st.caption("Extract SEC line items, view GAAP tags and descriptions, and map them to your own library.")

# ---------- Helpers ----------
def _db_has_statement_type(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(term_mappings)")
    cols = [r[1] for r in cur.fetchall()]
    return "statement_type" in cols

def fetch_saved_map(cik: str, statement_type: str) -> dict:
    """
    Return dict {us_gaap_tag: library_term} for this cik and statement_type.
    Works whether DB has statement_type or not.
    """
    if not os.path.exists(DB_PATH):
        return {}
    conn = sqlite3.connect(DB_PATH)
    try:
        has_stype = _db_has_statement_type(conn)
        cur = conn.cursor()
        if has_stype:
            cur.execute("""
                SELECT us_gaap_tag, library_term
                FROM term_mappings
                WHERE cik = ? AND statement_type = ? AND library_term IS NOT NULL AND TRIM(library_term) != ''
            """, (cik, statement_type))
        else:
            cur.execute("""
                SELECT us_gaap_tag, library_term
                FROM term_mappings
                WHERE cik = ? AND library_term IS NOT NULL AND TRIM(library_term) != ''
            """, (cik,))
        rows = cur.fetchall()
        return {tag: lib for tag, lib in rows if str(tag or "").strip() != ""}
    finally:
        conn.close()

def upsert_mappings(cik: str, company_name: str, statement_type: str, mappings: list[tuple[str, str]]):
    """
    Save mappings with best available method.
    """
    if HAVE_TYPED_SAVE:
        save_mappings_with_type(cik, company_name, statement_type, mappings)
    else:
        # Fallback schema without statement_type
        from utils.db_handler import save_mappings
        save_mappings(cik, company_name, mappings)

@st.cache_data
def list_local_companies():
    """
    Load JSONs from data/raw/Companies_urgent and return {display: path}
    """
    base_path = os.path.join("data", "raw", "Companies_urgent")
    companies = {}
    if not os.path.exists(base_path):
        st.error(f"Directory not found: {base_path}")
        return companies
    for file in sorted(os.listdir(base_path)):
        if file.endswith(".json"):
            path = os.path.join(base_path, file)
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                name = data.get("entityName", file.replace(".json", ""))
                cik = str(data.get("cik", file.replace(".json", "")))
                companies[f"{name} ({cik})"] = path
            except Exception as e:
                print(f"[WARN] Skipping {file}: {e}")
    return companies

def load_company_json(file_path: str):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return None

# ---------- Progress dashboard ----------
st.subheader("Company Mapping Progress Overview")
if get_company_progress_summary is not None:
    summary = get_company_progress_summary()
    if summary:
        for (name, cik), stats in summary.items():
            total = sum(stats.values())
            st.markdown(f"""
            <div class='progress-card'>
                <b>{name} ({cik})</b><br>
                <span class='badge income'>Income: {stats.get('income', 0)}</span>
                <span class='badge balance'>Balance: {stats.get('balance', 0)}</span>
                <span class='badge cashflow'>Cashflow: {stats.get('cashflow', 0)}</span>
                <span class='badge equity'>Equity: {stats.get('equity', 0)}</span>
                <span class='badge total'>Total: {total}</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No mappings saved yet. Start mapping to see progress here.")
else:
    st.info("Progress dashboard unavailable with the current database module.")
st.divider()

# ---------- Company selection ----------
local_companies = list_local_companies()
company_choice = st.selectbox(
    "Select a company (from data/raw/Companies_urgent):",
    ["-- Select --"] + list(local_companies.keys()),
    index=0
)

company_data = None
if company_choice != "-- Select --":
    company_data = load_company_json(local_companies[company_choice])

uploaded_json = st.file_uploader("Or upload your own company_facts.json", type="json")
if uploaded_json:
    company_data = json.load(uploaded_json)

# ---------- Main logic ----------
if company_data:
    cik = str(company_data.get("cik", ""))
    company_name = company_data.get("entityName", "Unknown")

    st.success(f"Loaded company: {company_name} (CIK: {cik})")

    if get_progress_for_company is not None:
        comp_progress = get_progress_for_company(cik)
        cols = st.columns(4)
        for i, stmt in enumerate(["income", "balance", "cashflow", "equity"]):
            with cols[i]:
                st.metric(stmt.title(), comp_progress.get(stmt, 0))
        st.divider()

    statement_choice = st.selectbox(
        "Select Statement Type:",
        ["income", "balance", "cashflow", "equity"],
        index=0
    )

    session_key = f"live_df_{cik}_{statement_choice}"

    if session_key not in st.session_state:
        with st.spinner("Parsing SEC filing..."):
            df = build_statement_table(cik, company_data, statement_type=statement_choice)

        if df is None or df.empty:
            st.error("Could not extract statement data.")
            st.stop()

        if "Library Term" not in df.columns:
            df["Library Term"] = ""

        # Prefill from DB
        saved_map = fetch_saved_map(cik, statement_type=statement_choice)
        if saved_map:
            df["Library Term"] = df["us-gaap Tag"].map(saved_map).fillna(df["Library Term"])
            st.info(f"Loaded {len(saved_map)} previously saved mappings for this statement.")

        st.session_state[session_key] = df.copy()

    working_df = st.session_state[session_key].copy()

    # 🔹 New: provide a helper dropdown per row without killing free-typing
    # We add a parallel "Pick (optional)" column with your existing terms.
    existing_terms = get_all_library_terms()
    if "Pick (optional)" not in working_df.columns:
        working_df["Pick (optional)"] = ""

    edited_df = st.data_editor(
        working_df,
        key=f"editor_{session_key}",
        width="stretch",
        num_rows="dynamic",
        column_config={
            # Keep Library Term fully editable
            "Library Term": st.column_config.TextColumn(
                "Library Term",
                help="Type a term freely. Or use the dropdown in 'Pick (optional)' to fill this cell.",
                max_chars=150
            ),
            # Add dropdown suggestions that can copy into Library Term
            "Pick (optional)": st.column_config.SelectboxColumn(
                "Pick (optional)",
                help="Optional helper: pick from saved terms to fill 'Library Term'.",
                options=existing_terms,
                required=False
            ),
        }
    )

    # Apply any picks into Library Term (so users can mix typing + picking)
    df_to_keep = edited_df.copy()
    if "Pick (optional)" in df_to_keep.columns:
        pick_mask = df_to_keep["Pick (optional)"].fillna("").astype(str).str.strip() != ""
        df_to_keep.loc[pick_mask, "Library Term"] = df_to_keep.loc[pick_mask, "Pick (optional)"]
        # Drop the helper column from what we persist/save (UI will re-add next render)
        df_to_keep = df_to_keep.drop(columns=["Pick (optional)"], errors="ignore")

    # Persist the user's latest edits across reruns
    st.session_state[session_key] = df_to_keep.copy()

    col_save, col_reset = st.columns([1, 1])
    with col_save:
        if st.button("Save mappings to database"):
            mappings = [
                (row["us-gaap Tag"], str(row["Library Term"]).strip())
                for _, row in df_to_keep.iterrows()
                if str(row["Library Term"]).strip() != ""
            ]
            if mappings:
                upsert_mappings(cik, company_name, statement_choice, mappings)
                st.success(f"Saved {len(mappings)} mappings for {company_name} ({statement_choice}).")
            else:
                st.warning("No library terms entered yet.")

    with col_reset:
        if st.button("Reset unsaved edits in table"):
            # Reload from DB + rebuild table from source
            with st.spinner("Resetting view..."):
                df = build_statement_table(cik, company_data, statement_type=statement_choice)
                if "Library Term" not in df.columns:
                    df["Library Term"] = ""
                saved_map = fetch_saved_map(cik, statement_choice)
                if saved_map:
                    df["Library Term"] = df["us-gaap Tag"].map(saved_map).fillna(df["Library Term"])
                st.session_state[session_key] = df.copy()
            st.rerun()

    st.caption(f"Database path: {os.path.abspath(DB_PATH)}")

else:
    st.info("Please select a company or upload a JSON file to begin.")

import io
import sqlite3
import pandas as pd

if os.path.exists(DB_PATH):
    # Create an in-memory copy for download
    with open(DB_PATH, "rb") as f:
        db_bytes = f.read()

    st.download_button(
        label="⬇️ Download Database File",
        data=db_bytes,
        file_name="company_mappings.db",
        mime="application/x-sqlite3"
    )

    # Optional preview
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = pd.read_sql_query("SELECT COUNT(*) as count FROM term_mappings", conn)["count"][0]
        st.write(f"💾 Database currently has {rows} records.")
        conn.close()
    except Exception as e:
        st.warning(f"Could not read database: {e}")
else:
    st.warning("⚠️ Database file not found.")
