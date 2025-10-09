import streamlit as st
import json
import os
import pandas as pd

from utils.xbrl_statement_parser import build_statement_table
from utils.db_handler import (
    init_db,
    save_mappings_with_type,
    get_all_library_terms,
    get_company_progress_summary,
    get_progress_for_company,
)

# ------------------------------------------------------
# 💡 Mobile-friendly style injection
# ------------------------------------------------------
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
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 8px;
            color: white;
            font-size: 13px;
            margin-right: 6px;
        }
        .income { background: #4caf50; }
        .balance { background: #2196f3; }
        .cashflow { background: #ff9800; }
        .equity { background: #9c27b0; }
        .total { background: #212121; }
        </style>
        <meta name="theme-color" content="#0a66c2">
    """, unsafe_allow_html=True)


# ------------------------------------------------------
# 📂 Load JSON files from /Companies_urgent
# ------------------------------------------------------
@st.cache_data
def list_local_companies():
    """
    Scans /data/raw/Companies_urgent/ for JSONs and returns:
    {display_name: full_path}
    """
    base_path = "/Users/jinenmodi/ImpData/financial_term_mapper/financial_term_mapper/data/raw/Companies_urgent"
    if not os.path.exists(base_path):
        st.error(f"❌ Directory not found: {base_path}")
        return {}

    companies = {}
    for file in sorted(os.listdir(base_path)):
        if not file.endswith(".json"):
            continue
        file_path = os.path.join(base_path, file)
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            name = data.get("entityName", file.replace(".json", ""))
            cik = str(data.get("cik", file.replace(".json", "")))
            display = f"{name} ({cik})"
            companies[display] = file_path
        except Exception as e:
            print(f"[WARN] Skipping {file}: {e}")
    return companies


@st.cache_data
def load_company_json(file_path: str):
    """Load a company JSON given its full path."""
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            st.info(f"Loaded company JSON: {os.path.basename(file_path)}")
            return json.load(f)
    st.error(f"File not found: {file_path}")
    return None


# ------------------------------------------------------
# 🏗️ INITIAL SETUP
# ------------------------------------------------------
st.set_page_config(page_title="Financial Term Mapper", layout="wide")
inject_mobile_styles()
init_db()

st.title("🏦 Financial Term Mapper")
st.caption("Extract SEC line items → view GAAP tags & descriptions → map them to your standardized library.")

# ------------------------------------------------------
# 📊 COLOR BADGE COMPANY DASHBOARD
# ------------------------------------------------------
st.subheader("📈 Company Mapping Progress Overview")

progress_data = get_company_progress_summary()

if progress_data:
    for comp in progress_data:
        st.markdown(f"""
        <div class='progress-card'>
            <b>{comp['company_name']} ({comp['cik']})</b><br>
            <span class='badge income'>Income: {comp.get('income', 0)}</span>
            <span class='badge balance'>Balance: {comp.get('balance', 0)}</span>
            <span class='badge cashflow'>Cashflow: {comp.get('cashflow', 0)}</span>
            <span class='badge equity'>Equity: {comp.get('equity', 0)}</span>
            <span class='badge total'>Total: {comp.get('total', 0)}</span>
        </div>
        """, unsafe_allow_html=True)
else:
    st.info("No mappings saved yet. Start mapping to see progress here.")

st.divider()


# ------------------------------------------------------
# 📁 Company selector (from Companies_urgent folder)
# ------------------------------------------------------
local_companies = list_local_companies()

company_choice = st.selectbox(
    "Select a company (from /Companies_urgent/):",
    options=["-- Select --"] + list(local_companies.keys()),
    index=0
)

company_data = None
if company_choice != "-- Select --":
    file_path = local_companies[company_choice]
    company_data = load_company_json(file_path)

# ------------------------------------------------------
# 📤 Optional upload
# ------------------------------------------------------
uploaded_json = st.file_uploader("Or upload your own company_facts.json", type="json")
if uploaded_json:
    company_data = json.load(uploaded_json)

# ------------------------------------------------------
# ⚙️ Main logic
# ------------------------------------------------------
if company_data:
    cik = str(company_data.get("cik", ""))
    company_name = company_data.get("entityName", "Unknown")

    st.success(f"✅ Loaded company: **{company_name}** (CIK: {cik})")

    # Show per-company progress before editing
    comp_progress = get_progress_for_company(cik)
    st.markdown("#### Current Progress:")
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

    with st.spinner("🔍 Parsing SEC filing..."):
        df = build_statement_table(cik, company_data, statement_type=statement_choice)

    if not df.empty:
        if "Library Term" not in df.columns:
            df["Library Term"] = ""

        st.markdown("### 🧾 Map Library Terms")
        st.write("Edit or select library terms to create your reusable mapping library.")

        existing_terms = get_all_library_terms()

        column_config = {
            "Library Term": st.column_config.SelectboxColumn(
                "Library Term",
                help="Choose existing or type a new one",
                options=existing_terms,
                required=False
            )
        }

        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            key=f"editor_{cik}_{statement_choice}",
            column_config=column_config,
            width="stretch"
        )

        if st.button("💾 Save Mappings to Database"):
            mappings = [
                (row["us-gaap Tag"], row["Library Term"])
                for _, row in edited_df.iterrows()
                if row["Library Term"].strip() != ""
            ]
            if mappings:
                save_mappings_with_type(cik, company_name, statement_choice, mappings)
                st.success(f"✅ Saved {len(mappings)} mappings for {company_name} ({statement_choice.title()}).")

                # Refresh progress after saving
                comp_progress = get_progress_for_company(cik)
                st.markdown("#### Updated Progress:")
                cols = st.columns(4)
                for i, stmt in enumerate(["income", "balance", "cashflow", "equity"]):
                    with cols[i]:
                        st.metric(stmt.title(), comp_progress.get(stmt, 0))
            else:
                st.warning("No library terms entered yet.")
    else:
        st.error("❌ Could not extract statement data.")
else:
    st.info("Please select a company or upload a JSON file to begin.")