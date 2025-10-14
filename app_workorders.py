# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders + Service Report + Service History
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Each page is isolated to avoid UI/state bleed-over
# - Optional Parquet cache (per-sheet parquet files)
# - “Data last updated” (ET) from latest XLSX commit
# --------------------------------------------------------------

from __future__ import annotations
import io, re, hashlib
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
import yaml
from zipfile import BadZipFile

APP_VERSION = "2025.10.15d"

# ---------- deps ----------
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed. Add it to requirements.txt"); st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.error("python-docx not installed. Add it to requirements.txt"); st.stop()

st.set_page_config(page_title="SPF Work Orders", page_icon="🧰", layout="wide")

# ---------- constants ----------
SHEET_WORKORDERS = "Workorders"          # history
SHEET_ASSET_MASTER = "Asset_Master"
SHEET_WO_MASTER = "Workorders_Master"    # listing sheet with flags
# Accept both variants for service history
SHEET_WO_SERVICE_CANDIDATES = ["Workorders_Master_Services", "Workorders_Master_service", "Workorders_Master_Service"]

SHEET_SERVICE_CANDIDATES = ["Service Report", "Service_Report", "ServiceReport"]
SHEET_USERS_CANDIDATES = ["Users", "Users]", "USERS", "users"]

REQUIRED_WO_COLS = ["WORKORDER","TITLE","STATUS","PO","P/N","QUANTITY RECEIVED","Vendors","COMPLETED ON","ASSET","Location"]
OPTIONAL_SORT_COL = "Sort"
ASSET_MASTER_COLS = ["Location","ASSET"]

MASTER_REQUIRED = [
    "ID","Title","Description","Asset","Status","Created on","Planned Start Date","Due date",
    "Started on","Completed on","Assigned to","Teams Assigned to","Completed by",
    "Location","IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"
]

SERVICE_REPORT_CANON = {
    "WO_ID": {"workorder","wo","work order","work order id","id","wo id"},
    "Title": {"title"},
    "Service": {"service","procedure","procedure name","task","step","line item"},
    "Asset": {"asset"},
    "Location": {"location","ns location","location2"},
    "Date": {"date","completed on","performed on","service date","closed on"},
    "User": {"user","technician","completed by","performed by","assigned to"},
    "Notes": {"notes","description","comment","comments","details"},
    "Status": {"status"},
    "Schedule": {"schedule","interval","frequency","meter interval","planned interval","cycle"},
    "Remaining": {"remaining","remaining value","units remaining","miles remaining","hours remaining","reading remaining","remaining units"},
    "Percent Remaining": {"percent remaining","% remaining","remaining %","remaining pct","pct remaining"},
    "Meter Type": {"meter type","type","uom","unit","units"},
    "Due Date": {"due date","next due","target date","next service date"},
}

SERVICE_HISTORY_CANON = {
    "WO_ID": {"id","wo","workorder","work order","workorder id"},
    "Title": {"title"},
    "Service": {"service","procedure name","procedure","task"},
    "Asset": {"asset"},
    "Location": {"location","ns location","location2"},
    "Date": {"completed on","performed on","date","service date"},
    "User": {"completed by","technician","assigned to","performed by","user"},
    "Notes": {"notes","description","comment","comments","details"},
    "Status": {"status"},
}

# ---------- helpers ----------
def to_plain(obj):
    if isinstance(obj, Mapping): return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [to_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    if "app_config" in st.secrets: return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:
        try: return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e: st.error(f"Invalid YAML in app_config_yaml secret: {e}"); return {}
    p = Path(__file__).resolve().parent / "app_config.yaml"
    if p.exists():
        try: return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except Exception as e: st.error(f"Invalid YAML in app_config.yaml: {e}"); return {}
    return {}

def download_bytes_from_github_file(*, repo: str, path: str, branch: str = "main", token: str | None = None) -> bytes:
    import requests
    def _headers(raw: bool = True):
        h = {"Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"}
        if token: h["Authorization"] = f"token {token}"
        return h
    url1 = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r1 = requests.get(url1, headers=_headers(raw=True), timeout=30)
    if r1.status_code == 200:
        data = r1.content
    else:
        url2 = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
        r2 = requests.get(url2, headers=_headers(raw=True), timeout=30)
        if r2.status_code != 200:
            raise RuntimeError(f"GitHub download failed ({r1.status_code}/{r2.status_code}). Check repo/path/branch/token.")
        data = r2.content
    if not data or len(data) < 100: raise RuntimeError("Downloaded file is unexpectedly small.")
    return data

def get_xlsx_bytes(cfg: dict) -> bytes:
    xlsx_path = (cfg.get("settings", {}) or {}).get("xlsx_path")
    if xlsx_path:
        p = Path(xlsx_path); 
        if not p.exists(): raise FileNotFoundError(f"Local Excel not found: {xlsx_path}")
        return p.read_bytes()
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh: raise RuntimeError("No [github] secrets found. Configure repo/path/branch/token.")
    return download_bytes_from_github_file(repo=gh.get("repo"), path=gh.get("path"), branch=gh.get("branch","main"), token=gh.get("token"))

def get_data_last_updated() -> str | None:
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh or not gh.get("repo") or not gh.get("path"): return None
    try:
        import requests
        from zoneinfo import ZoneInfo
        url = f"https://api.github.com/repos/{gh['repo']}/commits"
        params = {"path": gh["path"], "per_page": 1, "sha": gh.get("branch","main")}
        r = requests.get(url, headers={"Accept":"application/vnd.github+json"}, params=params, timeout=20); r.raise_for_status()
        iso = r.json()[0]["commit"]["committer"]["date"]
        dt_et = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(ZoneInfo("America/New_York"))
        return dt_et.strftime("Data last updated: %Y-%m-%d %H:%M ET")
    except Exception:
        return None

def _norm_date_any(v: str) -> str:
    s = (str(v) if v is not None else "").strip()
    if not s: return ""
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%d-%b-%Y","%Y-%m-%d %H:%M:%S"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception: pass
    try:
        dt = pd.to_datetime(s, errors="coerce")
        return "" if pd.isna(dt) else dt.strftime("%Y-%m-%d")
    except Exception:
        return s

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]; ws.autofilter(0,0, max(0,len(df)), max(0,len(df.columns)-1))
        for i, col in enumerate(df.columns):
            width = 12 if df.empty else min(60, max(10, int((df[col].astype(str).str.len().quantile(0.9) if not df[col].empty else 10)) + 2))
            ws.set_column(i, i, width)
    return buf.getvalue()

def to_docx_bytes(df: pd.DataFrame, title: str) -> bytes:
    doc = Document(); doc.styles["Normal"].font.name = "Calibri"; doc.styles["Normal"].font.size = Pt(10)
    doc.add_heading(title, level=1)
    rows, cols = len(df)+1, len(df.columns)
    tbl = doc.add_table(rows=rows, cols=cols); tbl.style = "Table Grid"
    for j, c in enumerate(df.columns): tbl.cell(0, j).text = str(c)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        for j, c in enumerate(df.columns): tbl.cell(i, j).text = "" if pd.isna(r[c]) else str(r[c])
    out = io.BytesIO(); doc.save(out); return out.getvalue()

def coerce_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool: return s
    m = s.astype(str).str.strip().str.lower()
    true_vals, false_vals = {"true","yes","y","1","t"}, {"false","no","n","0","f","","nan","none"}
    return m.map(lambda x: True if x in true_vals else False).astype(bool)

def _canonize_headers(df: pd.DataFrame, canon: dict[str, set[str]]) -> pd.DataFrame:
    low_to_orig = {str(c).strip().lower(): str(c) for c in df.columns}
    mapping = {}
    for key, aliases in canon.items():
        kl = key.lower()
        if kl in low_to_orig: mapping[low_to_orig[kl]] = key; continue
        for low, orig in low_to_orig.items():
            if (low in aliases) or (low.replace("  "," ") in aliases):
                mapping[orig] = key; break
    return df.rename(columns=mapping)

# ---------- Parquet helpers ----------
def _cfg_settings(cfg: dict):
    s = cfg.get("settings", {}) or {}
    return bool(s.get("use_parquet", False)), s.get("db_path", "")

def _pq_path(base: str, sheet: str) -> Path | None:
    if not base: return None
    p = Path(base)
    # store as one file per sheet: <base>.{sheet}.parquet
    suffix = ".parquet" if not str(p).endswith(".parquet") else ""
    return Path(str(p) + suffix + f".{sheet}.parquet")

def _xlsx_sig(xlsx_bytes: bytes) -> str:
    return hashlib.sha256(xlsx_bytes).hexdigest()[:12]

def get_sheet_df_from_cache_or_excel(*, xlsx_bytes: bytes, sheet: str, use_parquet: bool, db_path: str) -> pd.DataFrame:
    """Prefer parquet if present; else read Excel and (if enabled) write parquet."""
    pq = _pq_path(db_path, sheet) if use_parquet else None
    force_reload = st.session_state.get("_force_reload", False)
    if pq and pq.exists() and not force_reload:
        try:
            return pd.read_parquet(pq)
        except Exception:
            pass  # fall back to Excel
    # read Excel
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=

