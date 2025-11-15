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

# session key used when user clicks "Open" on dashboard
if "selected_company_cik" not in st.session_state:
    st.session_state["selected_company_cik"] = None


def inject_mobile_styles():
    st.markdown("""
    <style>
    .block-container {padding: 0.75rem 0.75rem 2.5rem 0.75rem !important;}

    div[data-baseweb="select"] > div {
        min-height: 44px !important;
    }
    button[kind="primary"], button[kind="secondary"] {
        min-height: 44px !important;
        font-size: 15px !important;
        padding: 0.35rem 0.9rem !important;
    }

    @media (max-width: 768px) {
        .stMarkdown, .stText, .stSelectbox, .stButton > button {
            font-size: 14px !important;
            line-height: 1.4em;
        }
        table {font-size: 12px !important;}
        .progress-card {
            padding: 8px 10px !important;
        }
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
    .badge {
        display:inline-block;
        padding:2px 8px;
        border-radius:8px;
        color:white;
        font-size:13px;
        margin-right:6px;
        margin-top:4px;
    }
    .income{background:#4caf50;}
    .balance{background:#2196f3;}
    .cashflow{background:#ff9800;}
    .equity{background:#9c27b0;}
    .total{background:#212121;}
    .status-done{color:#2e7d32;font-weight:bold;padding-left:6px;}
    .status-pending{color:#757575;font-weight:normal;padding-left:6px;}
    </style>
    """, unsafe_allow_html=True)


inject_mobile_styles()
st.title("Financial Term Mapper")
st.caption("Extract SEC line items, view GAAP tags and descriptions, and map them to your own library.")


# ------------------------- DB HELPERS -------------------------


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
            tag_norm = str(tag).strip().lower()
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
        conn = get_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute("SELECT cik, company_name FROM completed_companies")
                rows = cur.fetchall()
                return {str(cik): name for cik, name in rows}
            except mysql.connector.errors.ProgrammingError as e:
                if getattr(e, "errno", None) == 1146:
                    return {}
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


# ------------------------- FILE HELPERS -------------------------


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


@st.cache_data
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


# ------------------------- DASHBOARD -------------------------

# ------------------------- ENTERPRISE DASHBOARD (Version D) -------------------------

st.subheader("Company Mapping Progress Overview")

# Search Bar
search_query = st.text_input(
    "Search companies:",
    placeholder="Search by company name or CIK"
).strip().lower()

# Tabs
tab_pending, tab_completed, tab_all = st.tabs(["Pending", "Completed", "All Companies"])

completed = get_completed_companies()

# Fetch all company statistics once (performance improvement)
try:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT company_name, cik, statement_type, COUNT(DISTINCT us_gaap_tag)
        FROM term_mappings
        GROUP BY company_name, cik, statement_type
        ORDER BY company_name
    """)
    rows = cur.fetchall()
    conn.close()

    # Build summary
    summary = {}
    for name, cik, stype, count in rows:
        cik = str(cik)
        if (name, cik) not in summary:
            summary[(name, cik)] = {}
        summary[(name, cik)][stype] = count

except Exception as e:
    st.warning(f"Could not load dashboard: {e}")
    summary = {}

# Convert to list for easier filtering
company_list = []
for (name, cik), stats in summary.items():
    total = sum(stats.values())
    status = "DONE" if cik in completed else "PENDING"
    company_list.append({
        "name": name,
        "cik": cik,
        "income": stats.get("income", 0),
        "balance": stats.get("balance", 0),
        "cashflow": stats.get("cashflow", 0),
        "equity": stats.get("equity", 0),
        "total": total,
        "status": status
    })


# ---------- Helper: Filtering ----------
def apply_search(data):
    if not search_query:
        return data
    return [
        row for row in data
        if search_query in row["name"].lower() or search_query in row["cik"]
    ]


# ---------- Helper: Pagination ----------
def paginated_view(data, tab_container):
    items_per_page = 10
    total_pages = max(1, (len(data) + items_per_page - 1) // items_per_page)

    if f"page_{tab_container}" not in st.session_state:
        st.session_state[f"page_{tab_container}"] = 1

    page = st.session_state[f"page_{tab_container}"]

    col_prev, col_page, col_next = st.columns([1, 2, 1])
    with col_prev:
        if st.button("← Prev", key=f"prev_{tab_container}", disabled=(page == 1)):
            st.session_state[f"page_{tab_container}"] = page - 1
            st.rerun()

    with col_page:
        st.write(f"Page {page} of {total_pages}")

    with col_next:
        if st.button("Next →", key=f"next_{tab_container}", disabled=(page == total_pages)):
            st.session_state[f"page_{tab_container}"] = page + 1
            st.rerun()

    start = (page - 1) * items_per_page
    end = start + items_per_page
    return data[start:end]


# ---------- Render Cards ----------
def render_company_cards(data, tab_id):
    for global_index, row in enumerate(data):
        name, cik = row["name"], row["cik"]

        status_html = (
            f"<span class='status-done'>DONE</span>"
            if row["status"] == "DONE"
            else f"<span class='status-pending'>PENDING</span>"
        )

        col_card, col_btn = st.columns([5, 1])

        with col_card:
            st.markdown(f"""
                <div class='progress-card'>
                    <b>{name} ({cik})</b> {status_html}<br>
                    <span class='badge income'>Income: {row['income']}</span>
                    <span class='badge balance'>Balance: {row['balance']}</span>
                    <span class='badge cashflow'>Cashflow: {row['cashflow']}</span>
                    <span class='badge equity'>Equity: {row['equity']}</span>
                    <span class='badge total'>Total: {row['total']}</span>
                </div>
            """, unsafe_allow_html=True)

        with col_btn:
            unique_key = f"open_{cik}_{tab_id}_{global_index}"
            if st.button("Open", key=unique_key):
                st.session_state["selected_company_cik"] = cik
                st.rerun()


# ------------------------- TAB 1 — PENDING -------------------------
with tab_pending:
    pending_companies = [c for c in company_list if c["status"] == "PENDING"]
    pending_companies = apply_search(pending_companies)
    paginated = paginated_view(pending_companies, "pend")
    render_company_cards(paginated, "pending")

# ------------------------- TAB 2 — COMPLETED -------------------------
with tab_completed:
    completed_companies = [c for c in company_list if c["status"] == "DONE"]
    completed_companies = apply_search(completed_companies)
    paginated = paginated_view(completed_companies, "comp")
    render_company_cards(paginated, "completed")

# ------------------------- TAB 3 — ALL COMPANIES -------------------------
with tab_all:
    all_filtered = apply_search(company_list)
    paginated = paginated_view(all_filtered, "all")
    render_company_cards(paginated, "all")
# Completed companies list
st.subheader("Completed Companies")
if completed:
    for cik in sorted(completed):
        for cik, name in completed.items():
            st.markdown(f"- **{name} ({cik})**")
else:
    st.info("No companies completed yet.")

st.divider()

# ------------------------- COMPANY SELECTION -------------------------


industries = list_industries_and_companies()

# If user clicked "Open" above, try to auto-select matching industry / company
auto_cik = st.session_state.get("selected_company_cik")
auto_industry = None
auto_company_name = None

if auto_cik:
    for ind, comps in industries.items():
        for full_name in comps.keys():
            cik_value = full_name.split("(")[-1].rstrip(")")
            if str(cik_value) == str(auto_cik):
                auto_industry = ind
                auto_company_name = full_name
                break
        if auto_industry:
            break

industry_options = ["-- Select --"] + list(industries.keys())
industry_index = 0
if auto_industry and auto_industry in industries:
    industry_index = industry_options.index(auto_industry)

industry_choice = st.selectbox(
    "Select Industry:",
    industry_options,
    index=industry_index
)

company_data = None
company_choice = None
company_file_path = None

if industry_choice != "-- Select --":
    all_companies = industries[industry_choice]
    company_options = ["-- Select --"] + list(all_companies.keys())

    company_index = 0
    if auto_company_name and auto_company_name in all_companies:
        company_index = company_options.index(auto_company_name)

    company_choice = st.selectbox(
        "Select Company:",
        company_options,
        index=company_index
    )

    if company_choice != "-- Select --":
        company_file_path = all_companies[company_choice]
        company_data = load_company_json(company_file_path)

# Manual upload still allowed (overrides dropdown)
uploaded_json = st.file_uploader("Or upload your own company_facts.json", type="json")
if uploaded_json:
    company_data = json.load(uploaded_json)

# ------------------------- MAIN LOGIC -------------------------


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

    # ---------- ACTION BUTTONS (ORDERED) ----------
    st.divider()
    st.subheader("Export Company Mappings to JSON")

    btn_cols = st.columns(3)

    with btn_cols[0]:
        if st.button("Save mappings to database"):
            df_final = st.session_state.get(f"edited_{session_key}", df_to_keep)
            old_df = st.session_state[session_key]
            upsert_mappings_batch(cik, company_name, statement_choice, df_final, old_df)
            st.success(f"Saved {len(df_final)} mappings successfully for {company_name} ({statement_choice}).")

    with btn_cols[1]:
        if st.button("Download JSON Mappings"):
            json_data = export_mappings_to_json(cik)
            st.download_button(
                label="Click here to download JSON file",
                data=json_data,
                file_name=f"{company_name.replace(' ', '_')}_mappings.json",
                mime="application/json"
            )

    with btn_cols[2]:
        if company_choice and industry_choice != "-- Select --":
            if st.button("Mark Company as Completed"):
                mark_company_completed(
                    cik=str(company_data.get("cik", "")),
                    name=company_data.get("entityName", ""),
                    industry=industry_choice
                )
                st.success("Company marked as completed.")
                # keep selected so user can still edit later
                st.rerun()

else:
    st.info("Please select a company or upload a JSON file to begin.")