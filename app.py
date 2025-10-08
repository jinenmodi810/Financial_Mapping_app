import streamlit as st
import json
import pandas as pd

from utils.xbrl_statement_parser import build_statement_table
from utils.db_handler import init_db, save_mappings, get_all_library_terms

# ------------------------------------------------------
# INITIAL SETUP
# ------------------------------------------------------
st.set_page_config(page_title="Financial Term Mapper", layout="wide")
init_db()  # ensure DB is created

st.title("🏦 Financial Term Mapper")
st.write(
    "Extract SEC line items, view GAAP tags and descriptions, "
    "and map them to your own standardized library."
)

# ------------------------------------------------------
# FILE UPLOAD SECTION
# ------------------------------------------------------
uploaded_json = st.file_uploader("Upload company_facts.json", type="json")

if uploaded_json:
    company_data = json.load(uploaded_json)
    cik = str(company_data.get("cik", ""))
    company_name = company_data.get("entityName", "Unknown")

    st.info(f"Loaded company: **{company_name}** (CIK: {cik})")

    statement_choice = st.selectbox(
        "Select Statement Type",
        ["income", "balance", "cashflow", "equity"],
        index=0
    )

    # ------------------------------------------------------
    # PARSE SEC STATEMENT
    # ------------------------------------------------------
    with st.spinner("Parsing SEC filing..."):
        df = build_statement_table(cik, company_data, statement_type=statement_choice)

    if not df.empty:
        if "Library Term" not in df.columns:
            df["Library Term"] = ""

        st.write("🧾 Edit **Library Term** to build your reusable mapping library.")

        # ------------------------------------------------------
        # ✅ FETCH EXISTING LIBRARY TERMS BEFORE RENDERING TABLE
        # ------------------------------------------------------
        existing_terms = get_all_library_terms()

        # ------------------------------------------------------
        # CREATE EDITABLE TABLE WITH DROPDOWN
        # ------------------------------------------------------
        column_config = {
            "Library Term": st.column_config.SelectboxColumn(
                "Library Term",
                help="Choose from existing terms or type a new one",
                options=existing_terms,  # <-- dropdown values from DB
                required=False
            )
        }

        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key=f"editor_{cik}",
            column_config=column_config
        )

        # ------------------------------------------------------
        # SAVE MAPPINGS TO DATABASE
        # ------------------------------------------------------
        if st.button("💾 Save Mappings to Database"):
            mappings = [
                (row["us-gaap Tag"], row["Library Term"])
                for _, row in edited_df.iterrows()
                if row["Library Term"].strip() != ""
            ]
            if mappings:
                save_mappings(cik, company_name, mappings)
                st.success(f"Saved {len(mappings)} mappings for {company_name}.")
            else:
                st.warning("No library terms entered yet.")
    else:
        st.error("Could not extract statement data.")
else:
    st.info("Please upload a `company_facts.json` file to begin.")