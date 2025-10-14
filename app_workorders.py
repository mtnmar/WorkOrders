# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel/Parquet from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Sidebar: choose page
# - Top-of-page filters per page
# - Sheets used (if present):
#     Workorders                  (history)
#     Asset_Master                (assets per location)
#     Workorders_Master           (listing with IsOpen/IsOverdue/IsScheduled/IsCompleted/IsOld flags)
#     Workorders_Master_service   (service-performed lines per WO)  [Service History page]
#     Service Report              (flat service report; canonical names tolerated)
#     Users / Users]              (optional pick list of users)
# - Privacy: never reveal assignments outside allowed locations
# - Data last updated: latest XLSX/Parquet commit time shown in ET
# - Performance: tries Parquet first (fast), then falls back to Excel per sheet
# --------------------------------------------------------------

from __future__ import annotations
import io, re, os
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
import yaml
from zipfile import BadZipFile

APP_VERSION = "2025.10.15p"

# ---------- deps ----------
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed. Add it to requirements.txt")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.error("python-docx not installed. Add it to requirements.txt")
    st.stop()

st.set_page_config(page_title="SPF Work Orders", page_icon="🧰", layout="wide")

# ---------- constants ----------
SHEET_WORKORDERS       = "Workorders"         # history sheet
SHEET_ASSET_MASTER     = "Asset_Master"
SHEET_WO_MASTER        = "Workorders_Master"  # listing sheet with flags
SHEET_WO_SERVICE       = "Workorders_Master_service"   # service lines (for Service History)
SHEET_SERVICE_CANDIDATES = ["Service Report", "Service_Report", "ServiceReport"]  # report page source
SHEET_USERS_CANDIDATES = ["Users", "Users]", "USERS", "users"]

REQUIRED_WO_COLS = [
    "WORKORDER", "TITLE", "STATUS", "PO", "P/N", "QUANTITY RECEIVED",
    "Vendors", "COMPLETED ON", "ASSET", "Location",
]
OPTIONAL_SORT_COL = "Sort"
ASSET_MASTER_COLS = ["Location", "ASSET"]

MASTER_REQUIRED = [
    "ID","Title","Description","Asset","Status","Created on","Planned Start Date",
    "Due date","Started on","Completed on","Assigned to","Teams Assigned to",
    "Completed by","Location","IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"
]

# Canon for Service Report (used for Coming Due/Overdue logic)
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
    # due logic
    "Schedule": {"schedule","interval","frequency","meter interval","planned interval","cycle"},
    "Remaining": {"remaining","remaining value","units remaining","miles remaining","hours remaining","reading remaining","remaining units"},
    "Percent Remaining": {"percent remaining","% remaining","remaining %","remaining pct","pct remaining"},
    "Meter Type": {"meter type","type","uom","unit","units"},
    "Due Date": {"due date","next due","target date","next service date"},
}

# Canon for Service History (from Workorders_Master_service)
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

# Potential Parquet filenames (looked up in same repo folder as the XLSX)
PARQUET_CANDIDATES = {
    SHEET_ASSET_MASTER: ["Asset_Master.parquet"],
    SHEET_WO_MASTER: ["Workorders_Master.parquet"],
    SHEET_WO_SERVICE: ["Workorders_Master_service.parquet"],
    # Service Report allows several base names
    "Service Report": ["Service Report.parquet", "Service_Report.parquet", "ServiceReport.parquet"],
    SHEET_WORKORDERS: ["Workorders.parquet"],  # rarely used; we still keep the Excel fallback
    "Users": ["Users.parquet"],
}

# ---------- helpers ----------
def to_plain(obj):
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj

def load_config() -> dict:
    if "app_config" in st.secrets:
        return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:
        try:
            return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}")
            return {}
    here = Path(__file__).resolve().parent
    cfg_file = here / "app_config.yaml"
    if cfg_file.exists():
        try:
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    return {}

@st.cache_data(ttl=600, show_spinner=False)
def download_bytes_from_github_file(*, repo: str, path: str, branch: str = "main", token: str | None = None) -> bytes:
    import requests
    def _headers(raw: bool = True):
        h = {"Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"}
        if token:
            h["Authorization"] = f"token {token}"
        return h
    # contents API
    url1 = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r1 = requests.get(url1, headers=_headers(raw=True), timeout=30)
    if r1.status_code == 200:
        data = r1.content
    else:
        # raw fallback
        url2 = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
        r2 = requests.get(url2, headers=_headers(raw=True), timeout=30)
        if r2.status_code != 200:
            snippet1 = (r1.text or "")[:200]
            snippet2 = (r2.text or "")[:200]
            raise RuntimeError(
                f"GitHub download failed.\n"
                f"Contents API ({r1.status_code}): {snippet1}\n"
                f"Raw URL ({r2.status_code}): {snippet2}"
            )
        data = r2.content
    if not data or len(data) < 50:
        raise RuntimeError("Downloaded file is unexpectedly small. Check repo/path/branch/token.")
    return data

def _repo_ctx():
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh:
        raise RuntimeError("No [github] secrets found. Configure repo/path/branch/token.")
    repo = gh.get("repo")
    base_path = gh.get("path")
    branch = gh.get("branch", "main")
    token = gh.get("token")
    base_dir = base_path.rsplit("/", 1)[0] if "/" in base_path else ""
    return repo, base_dir, branch, token, base_path

def _path_join(base_dir: str, filename: str) -> str:
    if not base_dir:
        return filename
    return f"{base_dir.rstrip('/')}/{filename.lstrip('/')}"

def _pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except Exception:
        return False

@st.cache_data(ttl=600, show_spinner=False)
def get_parquet_bytes_if_present(logical_name: str) -> bytes | None:
    """Try to fetch a Parquet file for `logical_name` from the same repo folder as the XLSX."""
    # respect settings.use_parquet (default True)
    cfg = load_config()
    use_parquet = (cfg.get("settings", {}) or {}).get("use_parquet", True)
    if not use_parquet or not _pyarrow_available():
        return None
    try:
        repo, base_dir, branch, token, _ = _repo_ctx()
        candidates = PARQUET_CANDIDATES.get(logical_name, [])
        for fname in candidates:
            path = _path_join(base_dir, fname)
            try:
                data = download_bytes_from_github_file(repo=repo, path=path, branch=branch, token=token)
                # Quick sanity check (size)
                if data and len(data) > 50:
                    return data
            except Exception:
                continue
        return None
    except Exception:
        return None

@st.cache_data(ttl=600, show_spinner=False)
def get_xlsx_bytes(cfg: dict) -> bytes:
    """Fetch original XLSX bytes (source of truth) — cached."""
    xlsx_path = (cfg.get("settings", {}) or {}).get("xlsx_path")
    if xlsx_path:
        p = Path(xlsx_path)
        if not p.exists():
            raise FileNotFoundError(f"Local Excel not found: {xlsx_path}")
        return p.read_bytes()
    repo, base_dir, branch, token, xlsx_file = _repo_ctx()
    return download_bytes_from_github_file(repo=repo, path=xlsx_file, branch=branch, token=token)

@st.cache_data(ttl=300, show_spinner=False)
def get_data_last_updated() -> str | None:
    # Show latest commit time (XLSX or Parquet) in ET
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh or not gh.get("repo"):
        return None
    try:
        import requests
        from zoneinfo import ZoneInfo
        repo = gh["repo"]
        branch = gh.get("branch", "main")
        path = gh.get("path")
        repo_owner_repo = repo

        def latest_commit_for(path_):
            url = f"https://api.github.com/repos/{repo_owner_repo}/commits"
            params = {"path": path_, "per_page": 1, "sha": branch}
            headers = {"Accept": "application/vnd.github+json"}
            if gh.get("token"):
                headers["Authorization"] = f"token {gh['token']}"
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            js = r.json()
            if not js:
                return None
            return js[0]["commit"]["committer"]["date"]  # UTC Z

        # Prefer Parquet timestamps if present; else use XLSX
        base_dir = path.rsplit("/", 1)[0] if "/" in path else ""
        parquet_targets = [
            *PARQUET_CANDIDATES.get(SHEET_WO_MASTER, []),
            *PARQUET_CANDIDATES.get("Service Report", []),
            *PARQUET_CANDIDATES.get(SHEET_WO_SERVICE, []),
            *PARQUET_CANDIDATES.get(SHEET_ASSET_MASTER, []),
        ]
        iso = None
        for fname in parquet_targets:
            iso = latest_commit_for(_path_join(base_dir, fname))
            if iso:
                break
        if not iso:
            iso = latest_commit_for(path)
        if not iso:
            return None
        dt_utc = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        dt_et  = dt_utc.astimezone(ZoneInfo("America/New_York"))
        return dt_et.strftime("Data last updated: %Y-%m-%d %H:%M ET")
    except Exception:
        return None

def _norm_date_any(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
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
        ws = w.sheets[sheet]
        ws.autofilter(0, 0, max(0, len(df)), max(0, len(df.columns) - 1))
        for i, col in enumerate(df.columns):
            if df.empty:
                width = 12
            else:
                lens = df[col].astype(str).str.len()
                q = lens.quantile(0.9) if not lens.empty else 10
                q = 10 if pd.isna(q) else q
                width = min(60, max(10, int(q) + 2))
            ws.set_column(i, i, width)
    return buf.getvalue()

def to_docx_bytes(df: pd.DataFrame, title: str) -> bytes:
    doc = Document()
    doc.styles["Normal"].font.name = "Calibri"
    doc.styles["Normal"].font.size = Pt(10)
    doc.add_heading(title, level=1)
    rows, cols = len(df) + 1, len(df.columns)
    tbl = doc.add_table(rows=rows, cols=cols)
    tbl.style = "Table Grid"
    for j, c in enumerate(df.columns):
        tbl.cell(0, j).text = str(c)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        for j, c in enumerate(df.columns):
            v = "" if pd.isna(r[c]) else str(r[c])
            tbl.cell(i, j).text = v
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

def coerce_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s
    m = s.astype(str).str.strip().str.lower()
    true_vals  = {"true","yes","y","1","t"}
    false_vals = {"false","no","n","0","f","", "nan", "none"}
    out = m.map(lambda x: True if x in true_vals else (False if x in false_vals else False))
    return out.astype(bool)

def _canonize_headers(df: pd.DataFrame, canon: dict[str, set[str]]) -> pd.DataFrame:
    low_to_orig = {str(c).strip().lower(): str(c) for c in df.columns}
    mapping = {}
    for key, aliases in canon.items():
        key_l = key.lower()
        if key_l in low_to_orig:
            mapping[low_to_orig[key_l]] = key
            continue
        for low, orig in low_to_orig.items():
            low_norm = re.sub(r"\s+", " ", low)
            if (low in aliases) or (low_norm in aliases):
                mapping[orig] = key
                break
    return df.rename(columns=mapping)

# ---------- data loaders (Parquet fast-path, Excel fallback) ----------
@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    # history (we keep Excel unless you later export a Workorders.parquet)
    pq = get_parquet_bytes_if_present(SHEET_WORKORDERS)
    if pq and _pyarrow_available():
        df = pd.read_parquet(io.BytesIO(pq))
    else:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED_WO_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")
    cols = REQUIRED_WO_COLS[:]
    if OPTIONAL_SORT_COL in df.columns:
        cols = cols + [OPTIONAL_SORT_COL]
    df = df[cols].copy()
    df["COMPLETED ON"] = df["COMPLETED ON"].map(_norm_date_any)
    for c in df.columns:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df

@st.cache_data(show_spinner=False)
def load_asset_master_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    pq = get_parquet_bytes_if_present(SHEET_ASSET_MASTER)
    if pq and _pyarrow_available():
        df = pd.read_parquet(io.BytesIO(pq))
    else:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in ASSET_MASTER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")
    for c in ASSET_MASTER_COLS:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[(df["Location"] != "") & (df["ASSET"] != "")]
    return df[ASSET_MASTER_COLS].copy()

@st.cache_data(show_spinner=False)
def load_wo_master_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    pq = get_parquet_bytes_if_present(SHEET_WO_MASTER)
    if pq and _pyarrow_available():
        df = pd.read_parquet(io.BytesIO(pq))
    else:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    have = [c for c in MASTER_REQUIRED if c in df.columns]
    if have:
        df = df[have].copy()
    for dc in ("Created on","Planned Start Date","Due date","Started on","Completed on"):
        if dc in df.columns:
            df[dc] = df[dc].map(_norm_date_any)
    for bc in ("IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"):
        if bc in df.columns:
            df[bc] = coerce_bool(df[bc])
    for c in [x for x in ["ID","Title","Description","Asset","Status","Assigned to","Teams Assigned to","Completed by","Location"] if x in df.columns]:
        df[c] = df[c].astype(str).str.strip()
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()
    return df

# Service Report loader: returns (raw_df, canon_df, source_name)
@st.cache_data(show_spinner=False)
def load_service_report_df(xlsx_bytes: bytes):
    pq = get_parquet_bytes_if_present("Service Report")
    if pq and _pyarrow_available():
        raw = pd.read_parquet(io.BytesIO(pq))
        raw.columns = [str(c).strip() for c in raw.columns]
        canon = _canonize_headers(raw.copy(), SERVICE_REPORT_CANON)
        # normalize useful fields for due logic
        if "Date" in canon.columns:
            canon["Date"] = canon["Date"].map(_norm_date_any)
        if "Due Date" in canon.columns:
            canon["Due Date"] = canon["Due Date"].map(_norm_date_any)
        # numeric helpers
        for col, newcol in [("Schedule","__Schedule_num"), ("Remaining","__Remaining_num"), ("Percent Remaining","__PctRemain_num")]:
            if col in canon.columns:
                canon[newcol] = pd.to_numeric(canon[col].astype(str).str.replace("%","", regex=False), errors="coerce")
            else:
                canon[newcol] = pd.NA
        if "__PctRemain_num" in canon.columns:
            pr = pd.to_numeric(canon["__PctRemain_num"], errors="coerce")
            canon["__PctRemain_num"] = pr.where((pr.isna()) | (pr <= 1.0), pr / 100.0)
        if "Meter Type" in canon.columns:
            canon["__MeterType_norm"] = canon["Meter Type"].astype(str).str.strip().str.lower()
        else:
            canon["__MeterType_norm"] = ""
        if "Due Date" in canon.columns:
            canon["__Due_dt"] = pd.to_datetime(canon["Due Date"], errors="coerce")
        else:
            canon["__Due_dt"] = pd.NaT
        for c in [x for x in ["WO_ID","Title","Service","Asset","Location","User","Notes","Status"] if x in canon.columns]:
            canon[c] = canon[c].astype(str).str.strip()
        return raw, canon, "Service Report (parquet)"
    # Excel fallback (try multiple candidate sheet names)
    for nm in SHEET_SERVICE_CANDIDATES:
        try:
            raw = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=nm, dtype=str, keep_default_na=False, engine="openpyxl")
            raw.columns = [str(c).strip() for c in raw.columns]
            canon = _canonize_headers(raw.copy(), SERVICE_REPORT_CANON)
            if "Date" in canon.columns:
                canon["Date"] = canon["Date"].map(_norm_date_any)
            if "Due Date" in canon.columns:
                canon["Due Date"] = canon["Due Date"].map(_norm_date_any)
            for col, newcol in [("Schedule","__Schedule_num"), ("Remaining","__Remaining_num"), ("Percent Remaining","__PctRemain_num")]:
                if col in canon.columns:
                    canon[newcol] = pd.to_numeric(canon[col].astype(str).str.replace("%","", regex=False), errors="coerce")
                else:
                    canon[newcol] = pd.NA
            if "__PctRemain_num" in canon.columns:
                pr = pd.to_numeric(canon["__PctRemain_num"], errors="coerce")
                canon["__PctRemain_num"] = pr.where((pr.isna()) | (pr <= 1.0), pr / 100.0)
            if "Meter Type" in canon.columns:
                canon["__MeterType_norm"] = canon["Meter Type"].astype(str).str.strip().str.lower()
            else:
                canon["__MeterType_norm"] = ""
            if "Due Date" in canon.columns:
                canon["__Due_dt"] = pd.to_datetime(canon["Due Date"], errors="coerce")
            else:
                canon["__Due_dt"] = pd.NaT
            for c in [x for x in ["WO_ID","Title","Service","Asset","Location","User","Notes","Status"] if x in canon.columns]:
                canon[c] = canon[c].astype(str).str.strip()
            return raw, canon, nm
        except Exception:
            continue
    return None, None, None

# Service History loader (from Workorders_Master_service)
@st.cache_data(show_spinner=False)
def load_service_history_df(xlsx_bytes: bytes):
    pq = get_parquet_bytes_if_present(SHEET_WO_SERVICE)
    if pq and _pyarrow_available():
        df = pd.read_parquet(io.BytesIO(pq))
    else:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_WO_SERVICE, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    df = _canonize_headers(df, SERVICE_HISTORY_CANON)
    if "Date" in df.columns:
        df["Date"] = df["Date"].map(_norm_date_any)
    for c in [x for x in ["WO_ID","Title","Service","Asset","Location","User","Notes","Status"] if x in df.columns]:
        df[c] = df[c].astype(str).str.strip()
    return df

@st.cache_data(show_spinner=False)
def load_users_sheet(xlsx_bytes: bytes) -> list[str] | None:
    pq = get_parquet_bytes_if_present("Users")
    if pq and _pyarrow_available():
        dfu = pd.read_parquet(io.BytesIO(pq))
        dfu.columns = [str(c).strip() for c in dfu.columns]
        cols_low = {c.lower(): c for c in dfu.columns}
        col = cols_low.get("user")
        if not col:
            return None
        users = [u.strip() for u in dfu[col].astype(str).tolist() if str(u).strip()]
        return sorted(dict.fromkeys(users))
    for name in SHEET_USERS_CANDIDATES:
        try:
            dfu = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=name, dtype=str, keep_default_na=False, engine="openpyxl")
            cols_low = {c.lower(): c for c in dfu.columns}
            col = cols_low.get("user")
            if not col:
                continue
            users = [u.strip() for u in dfu[col].astype(str).tolist() if str(u).strip()]
            users = sorted(dict.fromkeys(users))
            return users
        except Exception:
            pass
    return None

# ---------- App ----------
st.sidebar.caption(f"SPF Work Orders — v{APP_VERSION}")

cfg = load_config()
cfg = to_plain(cfg)

# Auth
cookie_cfg = cfg.get("cookie", {})
auth = stauth.Authenticate(
    cfg.get("credentials", {}),
    cookie_cfg.get("name", "spf_workorders_portal"),
    cookie_cfg.get("key", "super_secret_key"),
    cookie_cfg.get("expiry_days", 7),
)

name, auth_status, username = auth.login("Login", "main")

if auth_status is False:
    st.error("Username/password is incorrect")
elif auth_status is None:
    st.info("Please log in.")
else:
    auth.logout("Logout", "sidebar")
    st.sidebar.success(f"Logged in as {name}")

    updated = get_data_last_updated()
    if updated:
        st.sidebar.caption(updated)

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    # Page choice (leave width for tables)
    page = st.sidebar.radio(
        "Page",
        ["🔎 Asset History", "📋 Work Orders", "🧾 Service Report", "📚 Service History"],
        index=1
    )

    # Load workbook bytes (Excel fallback source)
    try:
        xlsx_bytes = get_xlsx_bytes(cfg)
    except Exception as e:
        st.error(f"Could not load Excel: {e}")
        st.stop()

    # Access control: Locations
    try:
        df_am  = load_asset_master_df(xlsx_bytes, SHEET_ASSET_MASTER)
    except BadZipFile:
        st.error("The downloaded file isn’t a valid .xlsx. Check your [github] repo/path/branch/token.")
        st.stop()
    except Exception as e:
        st.error(f"Failed to read Asset_Master: {e}")
        st.stop()

    username_ci = str(username).casefold()
    admins_ci = {str(u).casefold() for u in (cfg.get("access", {}).get("admin_usernames", []) or [])}
    is_admin = username_ci in admins_ci

    ul_raw = (cfg.get("access", {}).get("user_locations", {}) or {})
    ul_map_ci = {str(k).casefold(): v for k, v in ul_raw.items()}
    allowed_cfg = ul_map_ci.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]
    star = any(str(a).strip() == "*" for a in allowed_cfg)

    all_locations = sorted(df_am["Location"].dropna().unique().tolist())
    allowed_locations = set(all_locations) if (is_admin or star) else {loc for loc in all_locations if loc in set(allowed_cfg)}
    if not allowed_locations:
        st.error("No Locations configured for your account. Ask an admin to update your access.")
        with st.expander("Locations present in Asset_Master"):
            st.write(all_locations)
        st.stop()

    # ========= Page: Asset History =========
    if page == "🔎 Asset History":
        st.markdown("### Asset History")
        c1, c2 = st.columns([2, 3])
        with c1:
            loc_options = sorted(allowed_locations)
            chosen_loc = st.selectbox("Location", options=loc_options, index=0)
        with c2:
            assets_for_loc = sorted(df_am.loc[df_am["Location"] == chosen_loc, "ASSET"].dropna().unique().tolist())
            chosen_asset = st.selectbox("Asset", options=assets_for_loc, index=0 if assets_for_loc else None)

        if not assets_for_loc:
            st.info("No assets for this Location.")
            st.stop()

        # Load history when needed
        try:
            df_all = load_workorders_df(xlsx_bytes, SHEET_WORKORDERS)
        except Exception as e:
            st.error(f"Failed to read Workorders (history): {e}")
            st.stop()

        df = df_all[(df_all["Location"] == chosen_loc) & (df_all["ASSET"] == chosen_asset)].copy()

        # Drop negative/zero part transactions (keeps nulls/non-part rows)
        if "QUANTITY RECEIVED" in df.columns and "P/N" in df.columns:
            qnum = pd.to_numeric(df["QUANTITY RECEIVED"], errors="coerce")
            is_part = df["P/N"].astype(str).str.strip().ne("")
            df = df[~(is_part & qnum.notna() & (qnum <= 0))].copy()

        # Order: WORKORDER ASC, then Sort ASC, then stable
        df["__row"] = range(len(df))
        if OPTIONAL_SORT_COL in df.columns:
            df["__sort_key"] = pd.to_numeric(df[OPTIONAL_SORT_COL], errors="coerce").fillna(1).astype(int)
        else:
            df["__sort_key"] = 1
        df.sort_values(by=["WORKORDER", "__sort_key", "__row"], ascending=[True, True, True], inplace=True)
        df.loc[df["__sort_key"].isin([2, 3]), "WORKORDER"] = ""

        drop_cols = ["__row", "__sort_key", OPTIONAL_SORT_COL]
        df_out = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

        st.dataframe(df_out, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button(
                label="⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(df_out, sheet="Workorders"),
                file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.xlsx".replace(" ", "_"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                label="⬇️ Word (.docx)",
                data=to_docx_bytes(df_out, title=f"Work Orders — {chosen_loc} — {chosen_asset}"),
                file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.docx".replace(" ", "_"),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

    # ========= Page: Work Orders (listing + 7-day planner) =========
    elif page == "📋 Work Orders":
        st.markdown("### Work Orders — Filtered Views (flags from workbook)")

        try:
            df_master = load_wo_master_df(xlsx_bytes, SHEET_WO_MASTER)
        except Exception as e:
            st.error(f"Failed to read '{SHEET_WO_MASTER}': {e}")
            st.stop()

        opt_users = load_users_sheet(xlsx_bytes)  # may be None

        # Restrict to allowed locations
        df_master = df_master[df_master["Location"].isin(allowed_locations)].copy()
        total_in_scope = len(df_master)

        # Filters
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        with c1:
            loc_values = sorted(df_master["Location"].dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values, index=0)
        df_scope = df_master if chosen_loc == loc_all_label else df_master[df_master["Location"] == chosen_loc].copy()

        with c2:
            if opt_users is not None:
                user_choices = ["— Any user —"] + opt_users
            else:
                derived_users = sorted([u for u in df_scope.get("Assigned to", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if u])
                user_choices = ["— Any user —"] + derived_users
            sel_user = st.selectbox("Assigned user", options=user_choices, index=0)

        with c3:
            raw_teams = df_scope.get("Teams Assigned to", pd.Series([], dtype=str)).fillna("").astype(str)
            token_set = set()
            for v in raw_teams:
                for t in re.split(r"[;,]", v):
                    t = t.strip()
                    if t:
                        token_set.add(t)
            team_opts = ["— Any team —"] + sorted(token_set)
            sel_team = st.selectbox("Team", options=team_opts, index=0)

        with c4:
            view = st.radio(
                "View",
                ["All", "Open", "Overdue", "Scheduled (Planning)", "Completed", "Old"],
                horizontal=True,
                index=1 if "IsOpen" in df_scope.columns else 0
            )

        if sel_user != "— Any user —":
            df_scope = df_scope[df_scope["Assigned to"].astype(str).str.strip() == sel_user].copy()
            if df_scope.empty:
                st.warning("User not found at this location (or no work orders match).")
                st.dataframe(df_scope, use_container_width=True, hide_index=True)
                st.stop()

        if sel_team != "— Any team —":
            def team_hit(s: str) -> bool:
                if not s: return False
                parts = {p.strip() for p in re.split(r"[;,]", str(s)) if p.strip()}
                return sel_team.strip() in parts
            df_scope = df_scope[df_scope["Teams Assigned to"].fillna("").astype(str).map(team_hit)].copy()

        def pick_view(df_in: pd.DataFrame) -> pd.DataFrame:
            if view == "All" or not {"IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"}.issubset(df_in.columns):
                return df_in
            col = {"Open":"IsOpen","Overdue":"IsOverdue","Scheduled (Planning)":"IsScheduled","Completed":"IsCompleted","Old":"IsOld"}[view]
            return df_in[df_in[col]].copy()

        df_view = pick_view(df_scope)

        def present(cols: list[str]) -> list[str]:
            return [c for c in cols if c in df_view.columns]

        cols_all        = present(["ID","Title","Description","Asset","Created on","Planned Start Date","Due date","Started on","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_open       = present(["ID","Title","Description","Asset","Created on","Due date","Assigned to","Teams Assigned to","Location"])
        cols_overdue    = present(["ID","Title","Description","Asset","Due date","Assigned to","Teams Assigned to","Location"])
        cols_sched      = present(["ID","Title","Description","Asset","Planned Start Date","Due date","Assigned to","Teams Assigned To","Location"])  # tolerate minor case
        if "Teams Assigned to" in df_view.columns and "Teams Assigned To" in cols_sched:
            cols_sched[cols_sched.index("Teams Assigned To")] = "Teams Assigned to"
        cols_completed  = present(["ID","Title","Description","Asset","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_old        = present(["ID","Title","Description","Asset","Created on","Due date","Completed on","Assigned to","Teams Assigned to","Location"])

        if view == "Open":
            use_cols = cols_open or df_view.columns.tolist()
            sort_keys = [k for k in ["Due date","Created on","Title"] if k in df_view.columns]
        elif view == "Overdue":
            use_cols = cols_overdue or df_view.columns.tolist()
            sort_keys = [k for k in ["Due date","Title"] if k in df_view.columns]
        elif view == "Scheduled (Planning)":
            use_cols = cols_sched or df_view.columns.tolist()
            sort_keys = [k for k in ["Planned Start Date","Due date","Title"] if k in df_view.columns]
        elif view == "Completed":
            use_cols = cols_completed or df_view.columns.tolist()
            sort_keys = [k for k in ["Completed on","Title"] if k in df_view.columns]
        elif view == "Old":
            use_cols = cols_old or df_view.columns.tolist()
            sort_keys = [k for k in ["Created on","Due date","Title"] if k in df_view.columns]
        else:
            use_cols = cols_all or df_view.columns.tolist()
            sort_keys = [k for k in ["Completed on","Due date","Planned Start Date","Created on","Title"] if k in df_view.columns]

        if sort_keys:
            df_view = df_view.sort_values(by=sort_keys, na_position="last")

        st.caption(f"In scope: {total_in_scope}  •  After location/user/team filters: {len(df_scope)}  •  Showing ({view}): {len(df_view)}")
        st.dataframe(df_view[use_cols], use_container_width=True, hide_index=True)

        # 7-day planner (scheduled)
        with st.expander("🗓️ 7-day Scheduled Planner (printable)", expanded=False):
            df_sched = df_scope[df_scope.get("IsScheduled", False)].copy()
            if df_sched.empty:
                st.info("No scheduled items.")
            else:
                df_sched["Planned Start Date"] = pd.to_datetime(df_sched["Planned Start Date"], errors="coerce")
                start_base = pd.to_datetime(datetime.now().date())
                dates = [start_base + timedelta(days=i) for i in range(7)]
                labels = [d.strftime("%a %m/%d") for d in dates]
                cols = st.columns(7)
                for i, d in enumerate(dates):
                    with cols[i]:
                        st.markdown(f"**{labels[i]}**")
                        day_rows = df_sched[df_sched["Planned Start Date"].dt.date == d.date()]
                        if day_rows.empty:
                            st.caption("—")
                        else:
                            show_cols = [c for c in ["ID","Title","Description","Asset","Assigned to","Location"] if c in day_rows.columns]
                            st.dataframe(day_rows[show_cols], use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button(
                "⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(df_view[use_cols], sheet="WorkOrders"),
                file_name=f"WorkOrders_{view.replace(' ','_')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "⬇️ Word (.docx)",
                data=to_docx_bytes(df_view[use_cols], title=f"Work Orders — {view}"),
                file_name=f"WorkOrders_{view.replace(' ','_')}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

    # ========= Page: Service Report (as-is + Coming Due + Overdue) =========
    elif page == "🧾 Service Report":
        st.markdown("### Service Report")
        raw_sr, canon_sr, source_sheet = load_service_report_df(xlsx_bytes)
        if raw_sr is None:
            st.warning("No 'Service Report' sheet found.")
            st.stop()

        # Restrict by allowed locations (only filter used on this page)
        loc_col = None
        for c in raw_sr.columns:
            if c.strip().lower() in {"location","ns location","location2"}:
                loc_col = c; break
        loc_values = []
        if loc_col:
            loc_values = sorted([v for v in raw_sr[loc_col].astype(str).unique().tolist() if v in allowed_locations])
        else:
            loc_values = sorted(allowed_locations)

        c1, = st.columns([3])
        with c1:
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label], index=0)

        if loc_col and chosen_loc != loc_all_label:
            raw_show = raw_sr[raw_sr[loc_col].astype(str) == chosen_loc].copy()
            canon_in_scope = canon_sr[canon_sr["Location"].astype(str) == chosen_loc] if "Location" in canon_sr.columns else canon_sr.copy()
        else:
            raw_show = raw_sr.copy()
            canon_in_scope = canon_sr.copy()

        t_report, t_due, t_over = st.tabs(["Report", "Coming Due", "Overdue"])

        # --- Report (as-is) ---
        with t_report:
            st.caption(f"Source: {source_sheet}  •  Rows: {len(raw_show)}  •  No filters other than Location.")
            st.dataframe(raw_show, use_container_width=True, hide_index=True)
            c1, c2, _ = st.columns([1,1,6])
            with c1:
                st.download_button(
                    "⬇️ Excel (.xlsx)", data=to_xlsx_bytes(raw_show, sheet="Service_Report"),
                    file_name="Service_Report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with c2:
                st.download_button(
                    "⬇️ Word (.docx)", data=to_docx_bytes(raw_show, title="Service Report — As Is"),
                    file_name="Service_Report.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )

        # Helper: compute filters for coming-due/overdue
        def _due_frames(df_can: pd.DataFrame):
            if df_can is None or df_can.empty:
                return pd.DataFrame(), pd.DataFrame()
            df = df_can.copy()
            # Threshold by meter type
            def row_threshold(r) -> float:
                mt = str(r.get("__MeterType_norm","")).lower()
                return 0.05 if ("mile" in mt) else 0.10

            # Coming Due:
            conds = []
            # Case A: have Schedule + Remaining
            condA = (
                df["__Schedule_num"].notna() & (pd.to_numeric(df["__Schedule_num"], errors="coerce") > 0) &
                df["__Remaining_num"].notna() &
                (pd.to_numeric(df["__Remaining_num"], errors="coerce") >= 0)
            )
            if condA.any():
                thrA = df.apply(row_threshold, axis=1)
                condA2 = pd.to_numeric(df["__Remaining_num"], errors="coerce") <= (pd.to_numeric(df["__Schedule_num"], errors="coerce") * thrA)
                conds.append(condA & condA2)

            # Case B: have Percent Remaining (0..1 after normalization)
            if "__PctRemain_num" in df.columns:
                condB = df["__PctRemain_num"].notna()
                if condB.any():
                    thrB = df.apply(row_threshold, axis=1)
                    condB2 = pd.to_numeric(df["__PctRemain_num"], errors="coerce") <= thrB
                    conds.append(condB & condB2)

            coming_due_mask = pd.Series(False, index=df.index)
            for c in conds:
                coming_due_mask = coming_due_mask | c

            coming_due = df[coming_due_mask].copy()

            # Overdue:
            today = pd.Timestamp.today().normalize()
            overdue_mask = pd.Series(False, index=df.index)
            if "__Remaining_num" in df.columns:
                overdue_mask = overdue_mask | (pd.to_numeric(df["__Remaining_num"], errors="coerce") < 0)
            if "__Due_dt" in df.columns:
                overdue_mask = overdue_mask | (pd.to_datetime(df["__Due_dt"], errors="coerce") < today)

            overdue = df[overdue_mask].copy()
            return coming_due, overdue

        coming_due_df, overdue_df = _due_frames(canon_in_scope)

        # compact column set for due/overdue tabs
        def present_due(df: pd.DataFrame, extra: list[str] = None):
            base = ["WO_ID","Title","Service","Asset","Location","Date","User","Status",
                    "Schedule","Remaining","Percent Remaining","Meter Type","Due Date","Notes"]
            if extra:
                base = base + extra
            cols = [c for c in base if c in df.columns]
            return df[cols] if cols else df

        with t_due:
            st.caption("Coming Due = Remaining ≤ 10% of Schedule (or ≤ 5% if Meter Type contains 'miles').")
            if coming_due_df.empty:
                st.info("No items are coming due based on the available columns (Schedule/Remaining/Percent Remaining).")
            else:
                show = present_due(coming_due_df)
                st.dataframe(show.sort_values(by=[c for c in ["Due Date","Percent Remaining","Remaining"] if c in show.columns], na_position="last"),
                             use_container_width=True, hide_index=True)
                c1, c2, _ = st.columns([1,1,6])
                with c1:
                    st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(show, sheet="Service_Coming_Due"),
                                       file_name="Service_Coming_Due.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with c2:
                    st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(show, title="Service — Coming Due"),
                                       file_name="Service_Coming_Due.docx",
                                       mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        with t_over:
            st.caption("Overdue = Remaining < 0, or Due Date earlier than today.")
            if overdue_df.empty:
                st.info("No overdue items found based on Remaining/Due Date.")
            else:
                show = present_due(overdue_df)
                st.dataframe(show.sort_values(by=[c for c in ["Due Date","Remaining"] if c in show.columns], na_position="last"),
                             use_container_width=True, hide_index=True)
                c1, c2, _ = st.columns([1,1,6])
                with c1:
                    st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(show, sheet="Service_Overdue"),
                                       file_name="Service_Overdue.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with c2:
                    st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(show, title="Service — Overdue"),
                                       file_name="Service_Overdue.docx",
                                       mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

    # ========= Page: Service History (from Workorders_Master_service) =========
    else:
        st.markdown("### Service History")
        df_hist = load_service_history_df(xlsx_bytes)
        if df_hist is None or df_hist.empty:
            st.warning("No 'Workorders_Master_service' sheet found.")
            st.stop()

        # Restrict to allowed locations
        if "Location" in df_hist.columns:
            df_hist = df_hist[df_hist["Location"].isin(allowed_locations)].copy()

        # Filters: Location + Asset
        c1, c2 = st.columns([2, 3])
        with c1:
            loc_values = sorted(df_hist.get("Location", pd.Series([], dtype=str)).dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label], index=0)

        if chosen_loc != loc_all_label and "Location" in df_hist.columns:
            scope = df_hist[df_hist["Location"] == chosen_loc].copy()
        else:
            scope = df_hist.copy()

        with c2:
            assets = sorted([a for a in scope.get("Asset", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if a])
            sel_asset = st.selectbox("Asset", options=assets, index=0 if assets else None)

        if not assets:
            st.info("No assets available in this Location.")
            st.stop()

        scope = scope[scope["Asset"] == sel_asset] if "Asset" in scope.columns else scope
        # Sort newest first by Date if present
        if "Date" in scope.columns:
            scope = scope.copy()
            scope["__Date_dt"] = pd.to_datetime(scope["Date"], errors="coerce")
            scope = scope.sort_values(by="__Date_dt", ascending=False, na_position="last")
            scope = scope.drop(columns="__Date_dt")

        show_cols = [c for c in ["Date","WO_ID","Title","Service","Asset","User","Location","Notes","Status"] if c in scope.columns]
        st.caption(f"Rows: {len(scope)}")
        st.dataframe(scope[show_cols] if show_cols else scope, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1,1,6])
        with c1:
            st.download_button(
                "⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(scope[show_cols] if show_cols else scope, sheet="Service_History"),
                file_name=f"Service_History_{sel_asset.replace(' ','_')}.xlsx" if assets else "Service_History.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "⬇️ Word (.docx)",
                data=to_docx_bytes(scope[show_cols] if show_cols else scope, title=f"Service History — {sel_asset}" if assets else "Service History"),
                file_name=f"Service_History_{sel_asset.replace(' ','_')}.docx" if assets else "Service_History.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )


