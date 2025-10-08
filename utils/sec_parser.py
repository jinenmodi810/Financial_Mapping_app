import json
import zipfile
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Any
import streamlit as st


class SECParser:
    def __init__(self):
        self.temp_dir = None

    def extract_zip_file(self, zip_file) -> Dict[str, str]:
        """Extract ZIP file and return list of valid JSON files with company info"""
        try:
            # Create temporary directory
            self.temp_dir = tempfile.mkdtemp()

            # Extract ZIP contents
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(self.temp_dir)

            json_files = {}
            company_tracker = set()  # Track unique CIKs
            invalid_files = 0

            for root, dirs, files in os.walk(self.temp_dir):
                for file in files:
                    if not file.endswith('.json'):
                        continue

                    file_path = os.path.join(root, file)

                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)

                        company_name = str(data.get('entityName', '')).strip()
                        ticker = str(data.get('ticker', '')).strip()
                        cik = str(data.get('cik', '')).strip()

                        # Skip entries missing company name or CIK
                        if not company_name or not cik:
                            invalid_files += 1
                            continue

                        # Skip duplicate CIKs
                        if cik in company_tracker:
                            continue

                        # Clean formatting
                        company_name_clean = company_name.title()
                        if ticker:
                            display_name = f"{company_name_clean} ({ticker})"
                        else:
                            display_name = f"{company_name_clean} (CIK: {cik})"

                        json_files[display_name] = file_path
                        company_tracker.add(cik)

                    except Exception as e:
                        print(f"Error reading {file}: {e}")
                        invalid_files += 1
                        continue

            print(f"DEBUG: Final valid companies: {list(json_files.keys())}")
            print(f"DEBUG: Invalid or skipped files: {invalid_files}")

            if invalid_files > 0:
                st.sidebar.warning(f"Skipped {invalid_files} invalid or duplicate JSON files.")

            return json_files

        except Exception as e:
            st.error(f"Error extracting ZIP file: {str(e)}")
            return {}

    def load_json_from_path(self, file_path: str) -> Dict[str, Any]:
        """Load SEC JSON file from file path"""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Error loading JSON file: {str(e)}")

    def extract_company_info(self, sec_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract basic company information"""
        try:
            return {
                "cik": str(sec_data.get("cik", "")),
                "entity_name": sec_data.get("entityName", ""),
                "ticker": sec_data.get("ticker", ""),
                "sic": str(sec_data.get("sic", "")),
                "sic_description": sec_data.get("sicDescription", ""),
                "fiscal_year_end": sec_data.get("fiscalYearEnd", "")
            }
        except Exception as e:
            return {"error": f"Error extracting company info: {str(e)}"}

    def extract_facts(self, sec_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all financial facts from SEC JSON"""
        facts_list = []

        try:
            facts = sec_data.get("facts", {})
            us_gaap = facts.get("us-gaap", {})

            for tag, tag_data in us_gaap.items():
                if "units" in tag_data:
                    units = tag_data["units"]

                    # Process USD values
                    if "USD" in units:
                        for fact in units["USD"]:
                            facts_list.append({
                                "tag": f"us-gaap:{tag}",
                                "label": tag_data.get("label", ""),
                                "description": tag_data.get("description", ""),
                                "value": fact.get("val"),
                                "end_date": fact.get("end"),
                                "start_date": fact.get("start"),
                                "accession": fact.get("accn"),
                                "form": fact.get("form"),
                                "period": self._determine_period(fact)
                            })

        except Exception as e:
            print(f"Error extracting facts: {str(e)}")

        return facts_list

    def _determine_period(self, fact: Dict[str, Any]) -> str:
        """Determine if this is quarterly or annual data"""
        form = fact.get("form", "")
        if "10-K" in form:
            return "Annual"
        elif "10-Q" in form:
            return "Quarterly"
        else:
            return "Other"

    def get_unique_terms(self, facts_list: List[Dict[str, Any]]) -> List[str]:
        """Get unique terms/labels from extracted facts"""
        terms = set()
        for fact in facts_list:
            if fact["label"]:
                terms.add(fact["label"])
        return sorted(list(terms))

    def cleanup_temp_files(self):
        """Clean up temporary files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)