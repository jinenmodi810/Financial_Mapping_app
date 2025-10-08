import requests
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
import pandas as pd
from typing import Dict, List, Optional, Tuple

SEC_HEADERS = {"User-Agent": "ResearchApp/1.0 (contact@example.com)"}

# Namespaces used in XBRL linkbases
NS = {
    "link": "http://www.xbrl.org/2003/linkbase",
    # Note: we DO NOT use 'xlink' in XPath. We access xlink attrs via Clark notation in Python.
}
XLINK_ROLE = "{http://www.w3.org/1999/xlink}role"
XLINK_LABEL = "{http://www.w3.org/1999/xlink}label"
XLINK_HREF = "{http://www.w3.org/1999/xlink}href"
XLINK_FROM = "{http://www.w3.org/1999/xlink}from"
XLINK_TO = "{http://www.w3.org/1999/xlink}to"


def _get_latest_accession(cik: str) -> Optional[str]:
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    r = requests.get(url, headers=SEC_HEADERS)
    if r.status_code != 200:
        return None
    recent = r.json().get("filings", {}).get("recent", {})
    for form, acc in zip(recent.get("form", []), recent.get("accessionNumber", [])):
        if form in ("10-K", "10-Q"):
            return acc
    return None


def _get_file_urls(cik: str, accession: str) -> Dict[str, str]:
    cik_num = str(int(cik))
    acc_clean = accession.replace("-", "")
    idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/index.json"
    r = requests.get(idx_url, headers=SEC_HEADERS)
    if r.status_code != 200:
        return {}
    items = r.json().get("directory", {}).get("item", [])
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/"
    urls: Dict[str, str] = {}
    for it in items:
        name = it.get("name", "")
        if name.endswith("_pre.xml"):
            urls["pre"] = base + name
        elif name.endswith("_lab.xml"):
            urls["lab"] = base + name
    return urls


def _fetch_xml(url: str) -> Optional[ET.Element]:
    r = requests.get(url, headers=SEC_HEADERS)
    if r.status_code != 200:
        return None
    try:
        return ET.fromstring(r.text)
    except ET.ParseError:
        return None


def _discover_statement_roles(pre_root: ET.Element) -> List[Tuple[str, str]]:
    """Return list of (role_uri, friendly_name) for presentation links that look like statements."""
    roles = []
    for link in pre_root.findall(".//link:presentationLink", NS):
        role_uri = link.attrib.get(XLINK_ROLE, "")
        low = role_uri.lower()
        if any(k in low for k in [
            "income", "operations", "comprehensiveincome",
            "financialposition", "balancesheet",
            "cashflow", "cashflows", "equity", "stockholders"
        ]):
            # Build a friendly label
            if any(k in low for k in ["income", "operations", "comprehensiveincome"]):
                nice = "Income Statement"
            elif any(k in low for k in ["financialposition", "balancesheet"]):
                nice = "Balance Sheet"
            elif any(k in low for k in ["cashflow", "cashflows"]):
                nice = "Cash Flow Statement"
            elif any(k in low for k in ["equity", "stockholders"]):
                nice = "Statement of Stockholders' Equity"
            else:
                nice = role_uri.split("/")[-1] if "/" in role_uri else role_uri
            roles.append((role_uri, nice))
    # Deduplicate while preserving order
    seen = set()
    out = []
    for r in roles:
        if r[0] not in seen:
            out.append(r)
            seen.add(r[0])
    return out


def _normalize_concept_from_href(href: str) -> str:
    """
    Convert href into a QName-like string. Examples:
      http://fasb.org/us-gaap/2024#Revenues -> us-gaap:Revenues
      us-gaap_Revenues -> us-gaap:Revenues
    """
    target = href.split("#", 1)[-1] if "#" in href else href
    if "_" in target and ":" not in target:
        pfx, local = target.split("_", 1)
        return f"{pfx}:{local}"
    return target


def _parse_presentation_for_role(pre_root: ET.Element, role_uri: str) -> List[Tuple[str, int]]:
    """
    For the given role URI, return an ordered, depth-annotated list of concept QNames:
      [(qname, depth), ...]
    Avoids @xlink in XPath; filters in Python.
    """
    ordered: List[Tuple[str, int]] = []
    for link in pre_root.findall(".//link:presentationLink", NS):
        if link.attrib.get(XLINK_ROLE) != role_uri:
            continue

        # Build locator label -> concept QName
        loc_to_qname: Dict[str, str] = {}
        for loc in link.findall("link:loc", NS):
            label = loc.attrib.get(XLINK_LABEL, "")
            href = loc.attrib.get(XLINK_HREF, "")
            concept = _normalize_concept_from_href(href)
            loc_to_qname[label] = concept

        # Build child relations with order
        children_of: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        to_nodes = set()
        from_nodes = set()
        for arc in link.findall("link:presentationArc", NS):
            frm = arc.attrib.get(XLINK_FROM, "")
            to = arc.attrib.get(XLINK_TO, "")
            order = float(arc.attrib.get("order", "0"))
            children_of[frm].append((to, order))
            from_nodes.add(frm)
            to_nodes.add(to)

        roots = [lbl for lbl in from_nodes if lbl not in to_nodes]
        # Depth-first traversal honoring 'order'
        for root_lbl in roots:
            stack = deque([(root_lbl, 0)])
            while stack:
                cur_lbl, depth = stack.pop()
                if cur_lbl in loc_to_qname:
                    ordered.append((loc_to_qname[cur_lbl], depth))
                for child_lbl, ordv in sorted(children_of.get(cur_lbl, []), key=lambda x: x[1], reverse=True):
                    stack.append((child_lbl, depth + 1))

    return ordered


# ---------------------------------------------------------------------
# ✅ Updated label extraction (company display names)
# ---------------------------------------------------------------------
def _labels_for_concepts(lab_root: ET.Element) -> Dict[str, str]:
    """
    Map concept QName -> best human-readable label using the filing's lab.xml.
    Priority:
      1) standard 'label' role
      2) any role that ends with '/label'
      3) 'terseLabel'
      4) first available label
    """
    concept_roles: Dict[str, Dict[str, str]] = {}

    for link in lab_root.findall(".//link:labelLink", NS):
        loc_to_concept: Dict[str, str] = {}
        for loc in link.findall("link:loc", NS):
            loc_label = loc.attrib.get(XLINK_LABEL, "")
            href = loc.attrib.get(XLINK_HREF, "")
            concept_qname = _normalize_concept_from_href(href)
            if loc_label:
                loc_to_concept[loc_label] = concept_qname

        # Collect label resources
        res_role_text: Dict[str, Tuple[str, str]] = {}
        for res in link.findall("link:label", NS):
            res_lbl = res.attrib.get(XLINK_LABEL, "")
            role = res.attrib.get(XLINK_ROLE, "")
            text = (res.text or "").strip()
            if res_lbl:
                res_role_text[res_lbl] = (role, text)

        # Wire locs to label resources
        for arc in link.findall("link:labelArc", NS):
            frm = arc.attrib.get(XLINK_FROM, "")
            to = arc.attrib.get(XLINK_TO, "")
            concept = loc_to_concept.get(frm)
            if not concept:
                continue
            role, text = res_role_text.get(to, ("", ""))
            if not text:
                continue
            role_map = concept_roles.setdefault(concept, {})
            if role not in role_map:
                role_map[role] = text

    # Choose best label per concept
    out: Dict[str, str] = {}
    def pick(role_map: Dict[str, str]) -> str:
        if "http://www.xbrl.org/2003/role/label" in role_map:
            return role_map["http://www.xbrl.org/2003/role/label"]
        for r, t in role_map.items():
            if r.endswith("/label"):
                return t
        for r, t in role_map.items():
            if "terseLabel" in r:
                return t
        return next(iter(role_map.values()))

    for concept, role_map in concept_roles.items():
        out[concept] = pick(role_map)

    return out
# ---------------------------------------------------------------------


def build_statement_table(cik: str, company_facts: dict, statement_type: str = "income") -> pd.DataFrame:
    """
    Build a table:
      SEC Line Item | us-gaap Tag | Description
    for the chosen statement type by parsing pre.xml + lab.xml
    and combining with company_facts for descriptions.
    """
    accession = _get_latest_accession(cik)
    if not accession:
        return pd.DataFrame(columns=["SEC Line Item", "us-gaap Tag", "Description"])

    urls = _get_file_urls(cik, accession)
    if "pre" not in urls or "lab" not in urls:
        return pd.DataFrame(columns=["SEC Line Item", "us-gaap Tag", "Description"])

    pre_root = _fetch_xml(urls["pre"])
    lab_root = _fetch_xml(urls["lab"])
    if pre_root is None or lab_root is None:
        return pd.DataFrame(columns=["SEC Line Item", "us-gaap Tag", "Description"])

    roles = _discover_statement_roles(pre_root)
    if not roles:
        return pd.DataFrame(columns=["SEC Line Item", "us-gaap Tag", "Description"])

    # Choose a role matching requested type
    target = statement_type.lower()
    role_uri = None
    for uri, friendly in roles:
        low = (uri + " " + friendly).lower()
        if target in ("income", "pnl", "operations") and any(k in low for k in ["income", "operations", "comprehensiveincome"]):
            role_uri = uri
            break
        if target in ("balance", "balancesheet", "financialposition") and any(k in low for k in ["balance", "financialposition"]):
            role_uri = uri
            break
        if target in ("cashflow", "cashflows") and any(k in low for k in ["cashflow", "cashflows"]):
            role_uri = uri
            break
        if target in ("equity", "stockholders") and any(k in low for k in ["equity", "stockholders"]):
            role_uri = uri
            break
    if role_uri is None:
        role_uri = roles[0][0]

    ordered = _parse_presentation_for_role(pre_root, role_uri)
    labels_map = _labels_for_concepts(lab_root)

    # Company facts for descriptions
    us_gaap = company_facts.get("facts", {}).get("us-gaap", {})
    us_gaap_lower = {k.lower(): v for k, v in us_gaap.items()}

    rows = []
    seen = set()
    for qname, depth in ordered:
        if qname in seen:
            continue
        seen.add(qname)
        label = labels_map.get(qname, qname.split(":")[-1])
        tag = ""
        desc = ""
        if qname.lower().startswith("us-gaap:"):
            tag = qname.split(":", 1)[-1]
            info = us_gaap_lower.get(tag.lower())
            if info:
                desc = info.get("description", "") or info.get("label", "")
        rows.append({
            "SEC Line Item": ("  " * depth) + label,
            "us-gaap Tag": tag,
            "Description": desc
        })

    return pd.DataFrame(rows)


def discover_available_statements(cik: str) -> List[str]:
    accession = _get_latest_accession(cik)
    if not accession:
        return []
    urls = _get_file_urls(cik, accession)
    if "pre" not in urls:
        return []
    pre_root = _fetch_xml(urls["pre"])
    if pre_root is None:
        return []
    roles = _discover_statement_roles(pre_root)
    return [friendly for _, friendly in roles]