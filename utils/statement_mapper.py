import requests
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st


def get_latest_filing_accession(cik: str):
    """Return accession number for the most recent 10-K or 10-Q filing."""
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    headers = {"User-Agent": "ResearchApp/1.0 (your_email@example.com)"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        st.warning("Unable to fetch submission index from SEC.")
        return None

    data = r.json()
    recent = data.get("filings", {}).get("recent", {})
    for form, accession in zip(recent.get("form", []), recent.get("accessionNumber", [])):
        if form in ("10-K", "10-Q"):
            return accession
    return None


def get_standard_statement_terms(cik: str, accession: str):
    """Extract human-readable SEC statement titles from FilingSummary.xml."""
    cik_stripped = str(int(cik))
    acc_clean = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik_stripped}/{acc_clean}/FilingSummary.xml"

    headers = {"User-Agent": "ResearchApp/1.0 (your_email@example.com)"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        st.warning("FilingSummary.xml not found for this filing.")
        return []

    tree = ET.fromstring(r.text)
    titles = []
    for report in tree.findall(".//Report"):
        short_name = report.findtext("ShortName", "").strip()
        if short_name and "statement" in short_name.lower():
            titles.append(short_name)
    return titles


def extract_gaap_terms(company_facts_json: dict):
    """Flatten us-gaap terms from company_facts.json."""
    us_gaap = company_facts_json.get("facts", {}).get("us-gaap", {})
    terms = []
    for tag, fact in us_gaap.items():
        terms.append({
            "us-gaap Tag": tag,
            "Description": fact.get("description", ""),
            "Label": fact.get("label", "")
        })
    return terms


def build_statement_mapping(cik: str, company_facts_json: dict):
    """Return DataFrame mapping SEC statement titles to us-gaap tags + descriptions."""
    accession = get_latest_filing_accession(cik)
    if not accession:
        return pd.DataFrame(columns=["SEC Standard Term", "us-gaap Tag", "Description"])

    statements = get_standard_statement_terms(cik, accession)
    gaap_terms = extract_gaap_terms(company_facts_json)

    # Combine them side by side (best-effort alignment by index length)
    rows = []
    for i in range(max(len(statements), len(gaap_terms))):
        rows.append({
            "SEC Standard Term": statements[i] if i < len(statements) else "",
            "us-gaap Tag": gaap_terms[i]["us-gaap Tag"] if i < len(gaap_terms) else "",
            "Description": gaap_terms[i]["Description"] if i < len(gaap_terms) else ""
        })
    return pd.DataFrame(rows)