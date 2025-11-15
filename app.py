import os
import json
import pandas as pd
import streamlit as st
import mysql.connector

from utils.xbrl_statement_parser import build_statement_table
from utils.db_handler import init_db, get_all_library_terms
from utils.json_exporter import export_mappings_to_json

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
    """
    Fetch saved mappings for a given company and statement type.
    Keep both 'us_gaap_tag' and 'library_term' exactly as stored in DB.
    Only normalize tags temporarily for matching; preserve original text fully.
    """
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

        mapping = {}
        for tag, lib in rows:
            if not tag:
                continue
            tag_norm = str(tag).strip().lower()  # temporary lowercase key for matching
            mapping[tag_norm] = str(lib).strip()
        return mapping
    finally:
        conn.close()

def fetch_global_mappings() -> dict:
    """
    Fetch us_gaap_tag -> library_term for ALL companies.
    Used to auto-fill library terms for any company
    that shares the same us-gaap tag.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT us_gaap_tag, library_term
            FROM term_mappings
            WHERE library_term IS NOT NULL
              AND TRIM(library_term) != ''
        """)
        rows = cur.fetchall()

        global_map = {}
        for tag, lib in rows:
            if not tag:
                continue
            global_map[str(tag).strip()] = str(lib).strip()
        return global_map
    finally:
        conn.close()

def mark_company_completed(cik, name, industry):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO completed_companies (cik, company_name, industry)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE completed_at = CURRENT_TIMESTAMP
    """, (cik, name, industry))
    conn.commit()
    cur.close()
    conn.close()


def get_completed_companies():
    """
    Return a set of CIKs that are already marked as completed.
    If the completed_companies table does not exist yet,
    return an empty set so the app still runs.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT cik FROM completed_companies")
            rows = cur.fetchall()
            return {str(r[0]) for r in rows}
        except mysql.connector.errors.ProgrammingError as e:
            # 1146 = table does not exist
            if getattr(e, "errno", None) == 1146:
                return set()
            raise
        finally:
            cur.close()
    finally:
        conn.close()

def upsert_mappings_batch(cik: str, company_name: str, statement_type: str, new_df, old_df):
    """Batch update and delete mappings based on full table diff."""
    conn = get_connection()
    cur = conn.cursor()

    def normalize_tag(tag: str) -> str:
        tag = str(tag).strip()
        if not tag:
            return ""
        return tag

    try:
        old_map = {
            normalize_tag(r["us-gaap Tag"]): str(r["Library Term"]).strip()
            for _, r in old_df.iterrows()
            if str(r["Library Term"]).strip() != ""
        }
        new_map = {
            normalize_tag(r["us-gaap Tag"]): str(r["Library Term"]).strip()
            for _, r in new_df.iterrows()
            if str(r["Library Term"]).strip() != ""
        }

        deleted_tags = [t for t in old_map if t not in new_map]

        if deleted_tags:
            cur.executemany(
                """
                DELETE FROM term_mappings
                WHERE cik = %s AND statement_type = %s AND us_gaap_tag = %s
                """,
                [(cik, statement_type, t) for t in deleted_tags],
            )

        for tag, lib in new_map.items():
            cur.execute(
                """
                INSERT INTO term_mappings (cik, company_name, statement_type, us_gaap_tag, library_term)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE library_term = VALUES(library_term)
                """,
                (cik, company_name, statement_type, tag, lib),
            )

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
def list_industries_and_companies(base_dir="data/raw/by_industry_sp500"):
    industries = {}
    if not os.path.exists(base_dir):
        return industries

    for industry in sorted(os.listdir(base_dir)):
        industry_path = os.path.join(base_dir, industry)
        if not os.path.isdir(industry_path):
            continue

        companies = {}
        for file in sorted(os.listdir(industry_path)):
            if file.endswith(".json"):
                fp = os.path.join(industry_path, file)
                try:
                    with open(fp, "r") as f:
                        data = json.load(f)
                    name = data.get("entityName", file.replace(".json", ""))
                    cik = str(data.get("cik", file.replace(".json", "")))
                    companies[f"{name} ({cik})"] = fp
                except Exception as e:
                    print(f"[WARN] Could not load {file}: {e}")

        industries[industry] = companies

    return industries

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
# ---------- Company selection with Industry ----------
industries = list_industries_and_companies()
completed = get_completed_companies()

for ind in industries:
    industries[ind] = {
        name: fp for name, fp in industries[ind].items()
        if name.split("(")[-1].strip(")") not in completed
    }

industry_choice = st.selectbox(
    "Select Industry:",
    ["-- Select --"] + list(industries.keys()),
    index=0
)

company_data = None
company_choice = None
company_file_path = None

if industry_choice != "-- Select --":
    all_companies = industries[industry_choice]

    company_choice = st.selectbox(
        "Select Company:",
        ["-- Select --"] + list(all_companies.keys()),
        index=0
    )

    if company_choice != "-- Select --":
        company_file_path = all_companies[company_choice]
        with open(company_file_path, "r") as f:
            company_data = json.load(f)

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
        # Load global mappings for all companies
        global_map = fetch_global_mappings()

        # Temporary normalized key column
        df["_key"] = df["us-gaap Tag"].astype(str).str.strip()

        # Apply global mapping only where library term is still empty
        df["Library Term"] = df.apply(
            lambda row: (
                saved_map.get(row["_key"].lower(), "") or
                global_map.get(row["_key"], "") or
                row["Library Term"]
        ),
            axis=1
        )

        df = df.drop(columns=["_key"], errors="ignore")
        df["us-gaap Tag"] = df["us-gaap Tag"].astype(str).str.strip()
        if saved_map:
            df["_key"] = df["us-gaap Tag"].str.lower()
            df["Library Term"] = df["_key"].map(saved_map).fillna(df["Library Term"])
            df = df.drop(columns=["_key"], errors="ignore")
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
                
                global_map = fetch_global_mappings()

                df["_key"] = df["us-gaap Tag"].astype(str).str.strip()
                df["Library Term"] = df.apply(
                lambda row: (
                    saved_map.get(row["_key"].lower(), "") or
                    global_map.get(row["_key"], "") or
                    row["Library Term"]
                ),
                    axis=1
                )
                df = df.drop(columns=["_key"], errors="ignore")
                if saved_map:
                    df["_key"] = df["us-gaap Tag"].astype(str).str.strip().str.lower()
                    df["Library Term"] = df["_key"].map(saved_map).fillna(df["Library Term"])
                    df = df.drop(columns=["_key"], errors="ignore")
                st.session_state[session_key] = df.copy()
            st.rerun()
else:
    st.info("Please select a company or upload a JSON file to begin.")

st.divider()
st.subheader("Export Company Mappings to JSON")
# Mark company as completed
if company_data and company_choice and industry_choice != "-- Select --":
    if st.button("Mark Company as Completed"):
        mark_company_completed(
            cik=str(company_data.get("cik", "")),
            name=company_data.get("entityName", ""),
            industry=industry_choice
        )
        st.success("Company marked as completed.")
        st.rerun()

if company_data:
    if st.button("Download JSON Mappings"):
        json_data = export_mappings_to_json(cik)
        st.download_button(
            label="Click here to download JSON file",
            data=json_data,
            file_name=f"{company_name.replace(' ', '_')}_mappings.json",
            mime="application/json"
        )