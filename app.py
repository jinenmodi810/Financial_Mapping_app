import os
import json
import pandas as pd
import streamlit as st
import mysql.connector

from utils.xbrl_statement_parser import build_statement_table
from utils.db_handler import init_db, get_all_library_terms

try:
    from utils.db_handler import (
        save_mappings_with_type,
        get_company_progress_summary,
        get_progress_for_company,
    )
    HAVE_TYPED_SAVE = True
except Exception:
    from utils.db_handler import save_mappings
    HAVE_TYPED_SAVE = False
    get_company_progress_summary = None
    get_progress_for_company = None


def get_connection():
    return mysql.connector.connect(
        host="fintasenseai.mysql.database.azure.com",
        user="ayoub",
        password="Password@2025",
        database="fsfinancialmapper",
        port=3306,
        ssl_disabled=False,
        autocommit=True
    )


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
st.title("Financial Term Mapper")
st.caption("Extract SEC line items, view GAAP tags and descriptions, and map them to your own library.")


def fetch_saved_map(cik: str, statement_type: str) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT us_gaap_tag, library_term
            FROM term_mappings
            WHERE cik = %s AND statement_type = %s
              AND library_term IS NOT NULL AND TRIM(library_term) != ''
        """, (cik, statement_type))
        rows = cur.fetchall()
        return {str(tag).strip().lower(): str(lib).strip() for tag, lib in rows if str(tag or "").strip() != ""}
    finally:
        conn.close()


def upsert_mappings_batch(cik: str, company_name: str, statement_type: str, new_df, old_df):
    """Batch update and delete mappings based on full table diff."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Detect deleted tags (were mapped before but now cleared)
        deleted_tags = []
        old_map = {r["us-gaap Tag"].strip().lower(): r["Library Term"].strip().lower()
                   for _, r in old_df.iterrows() if str(r["Library Term"]).strip() != ""}
        new_map = {r["us-gaap Tag"].strip().lower(): r["Library Term"].strip().lower()
                   for _, r in new_df.iterrows() if str(r["Library Term"]).strip() != ""}

        for tag in old_map:
            if tag not in new_map:
                deleted_tags.append(tag)

        # Apply deletions
        if deleted_tags:
            cur.executemany("""
                DELETE FROM term_mappings
                WHERE cik = %s AND statement_type = %s AND LOWER(us_gaap_tag) = %s
            """, [(cik, statement_type, t) for t in deleted_tags])

        # Apply upserts
        for tag, lib in new_map.items():
            cur.execute("""
                INSERT INTO term_mappings (cik, company_name, statement_type, us_gaap_tag, library_term)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE library_term = VALUES(library_term)
            """, (cik, company_name, statement_type, tag, lib))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] batch upsert failed: {e}")
    finally:
        cur.close()
        conn.close()


@st.cache_data
def list_local_companies():
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
try:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT company_name, cik, statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        GROUP BY company_name, cik, statement_type
    """)
    rows = cur.fetchall()
    conn.close()
    if rows:
        summary = {}
        for name, cik, stype, count in rows:
            summary.setdefault((name, cik), {})[stype] = count
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
except Exception as e:
    st.warning(f"Could not load dashboard: {e}")

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

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings WHERE cik = %s GROUP BY statement_type
    """, (cik,))
    rows = cur.fetchall()
    conn.close()
    comp_progress = {stype: count for stype, count in rows}

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
        saved_map = fetch_saved_map(cik, statement_type=statement_choice)
        df["us-gaap Tag"] = df["us-gaap Tag"].astype(str).str.strip().str.lower()
        if saved_map:
            df["Library Term"] = df["us-gaap Tag"].map(saved_map).fillna(df["Library Term"])
            st.info(f"Loaded {len(saved_map)} previously saved mappings for this statement.")
        st.session_state[session_key] = df.copy()

    working_df = st.session_state[session_key].copy().fillna("")
    if "Pick (optional)" not in working_df.columns:
        working_df["Pick (optional)"] = ""

    desired_order = []
    for col in ["SEC Line Item", "us-gaap Tag", "Library Term", "Pick (optional)", "Description"]:
        if col in working_df.columns:
            desired_order.append(col)
    for col in working_df.columns:
        if col not in desired_order:
            desired_order.append(col)
    working_df = working_df[desired_order]

    existing_terms = get_all_library_terms()

    edited_df = st.data_editor(
        working_df,
        key=f"editor_{session_key}",
        width="stretch",
        num_rows="dynamic",
        column_config={
            "Library Term": st.column_config.TextColumn(
                "Library Term",
                help="Type a term freely. Or use the dropdown in 'Pick (optional)' to fill this cell.",
                max_chars=150
            ),
            "Pick (optional)": st.column_config.SelectboxColumn(
                "Pick (optional)",
                help="Optional helper: pick from saved terms to fill 'Library Term'.",
                options=existing_terms,
                required=False
            ),
        }
    )

    df_to_keep = edited_df.copy()
    if "Pick (optional)" in df_to_keep.columns:
        pick_mask = df_to_keep["Pick (optional)"].fillna("").astype(str).str.strip() != ""
        df_to_keep.loc[pick_mask, "Library Term"] = df_to_keep.loc[pick_mask, "Pick (optional)"]
        df_to_keep = df_to_keep.drop(columns=["Pick (optional)"], errors="ignore")

    st.session_state[f"edited_{session_key}"] = df_to_keep.copy()

    message_box = st.empty()
    col_save, col_reset = st.columns([1, 1])
    with col_save:
        if st.button("Save mappings to database"):
            df_final = st.session_state.get(f"edited_{session_key}", df_to_keep)
            old_df = st.session_state[session_key]
            upsert_mappings_batch(cik, company_name, statement_choice, df_final, old_df)
            message_box.success(f"✅ Saved {len(df_final)} mappings successfully for {company_name} ({statement_choice}).")
            st.session_state[session_key] = df_final.copy()
            st.rerun()

    with col_reset:
        if st.button("Reset unsaved edits in table"):
            with st.spinner("Resetting view..."):
                df = build_statement_table(cik, company_data, statement_type=statement_choice)
                if "Library Term" not in df.columns:
                    df["Library Term"] = ""
                saved_map = fetch_saved_map(cik, statement_choice)
                if saved_map:
                    df["us-gaap Tag"] = df["us-gaap Tag"].astype(str).str.strip().str.lower()
                    df["Library Term"] = df["us-gaap Tag"].map(saved_map).fillna(df["Library Term"])
                st.session_state[session_key] = df.copy()
            st.rerun()
else:
    st.info("Please select a company or upload a JSON file to begin.")