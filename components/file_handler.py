# components/file_handler.py
import streamlit as st
from utils.sec_parser import SECParser
from config.settings import MAX_FILE_SIZE, SUPPORTED_ARCHIVE_TYPES

def handle_zip_upload():
    """Handle ZIP file upload containing multiple SEC JSON files"""
    
    st.sidebar.header("📁 Industry Data Upload")
    
    uploaded_file = st.sidebar.file_uploader(
        "Upload ZIP file with SEC JSON files",
        type=SUPPORTED_ARCHIVE_TYPES,
        help=f"Upload a ZIP file containing JSON files for multiple companies. Max size: {MAX_FILE_SIZE}MB"
    )
    
    if uploaded_file is not None:
        try:
            # Check file size
            file_size = len(uploaded_file.getvalue()) / (1024 * 1024)  # MB
            if file_size > MAX_FILE_SIZE:
                st.sidebar.error(f"File size ({file_size:.1f}MB) exceeds maximum allowed size ({MAX_FILE_SIZE}MB)")
                return None, {}
            
            st.sidebar.success("✅ ZIP file uploaded successfully!")
            
            # Initialize parser
            parser = SECParser()
            
            # Extract ZIP and get list of companies
            with st.spinner("Extracting ZIP file and reading company data..."):
                company_files = parser.extract_zip_file(uploaded_file)
            
            if not company_files:
                st.sidebar.error("❌ No JSON files found in the ZIP archive")
                return None, {}
            
            st.sidebar.success(f"📊 Found {len(company_files)} companies")
            
            # Company selection dropdown
            st.sidebar.header("🏢 Select Company")
            selected_company = st.sidebar.selectbox(
                "Choose a company to map:",
                options=list(company_files.keys()),
                index=0
            )
            
            if selected_company:
                # Load selected company's data
                selected_file_path = company_files[selected_company]
                
                try:
                    with st.spinner(f"Loading data for {selected_company}..."):
                        sec_data = parser.load_json_from_path(selected_file_path)
                        company_info = parser.extract_company_info(sec_data)
                        facts_list = parser.extract_facts(sec_data)
                        unique_terms = parser.get_unique_terms(facts_list)
                    
                    # Display selected company info in sidebar
                    st.sidebar.markdown("---")
                    st.sidebar.write("**Selected Company Info:**")
                    st.sidebar.write(f"**Name:** {company_info.get('entity_name', 'N/A')}")
                    st.sidebar.write(f"**Ticker:** {company_info.get('ticker', 'N/A')}")
                    st.sidebar.write(f"**CIK:** {company_info.get('cik', 'N/A')}")
                    st.sidebar.write(f"**Industry:** {company_info.get('sic_description', 'N/A')}")
                    st.sidebar.write(f"**Total Facts:** {len(facts_list)}")
                    st.sidebar.write(f"**Unique Terms:** {len(unique_terms)}")
                    
                    return parser, {
                        "selected_company": selected_company,
                        "company_info": company_info,
                        "facts_list": facts_list,
                        "unique_terms": unique_terms,
                        "raw_data": sec_data,
                        "all_companies": list(company_files.keys()),
                        "total_companies": len(company_files)
                    }
                    
                except Exception as e:
                    st.sidebar.error(f"❌ Error loading company data: {str(e)}")
                    return None, {}
            
            return parser, {"all_companies": list(company_files.keys()), "total_companies": len(company_files)}
            
        except Exception as e:
            st.sidebar.error(f"❌ Error processing ZIP file: {str(e)}")
            return None, {}
    
    return None, {}
