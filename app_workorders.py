# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Sidebar: choose page
# - Top-of-page filters for each page
# - Sheets used (if present in the workbook):
#     Workorders              (history)
#     Asset_Master            (assets per location)
#     Workorders_Master       (listing with IsOpen/IsOverdue/IsScheduled/IsCompleted/IsOld flags)
#     Workorders_Master_service  (service-performed lines per WO)  [preferred]
#     Service Report          (fallback service-performed table)
#     Meters_Master           (meter definitions / current snapshot)
#     Reading_History         (meter reading history)
#     Users / Users]          (optional pick list of users)
# - Privacy: never reveal assignments outside allowed locations
# - Data last updated: latest XLSX commit time shown in ET
# --------------------------------------------------------------

from __future__ import annotations
import io, re
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone, timedelta
from zipfile import BadZipFile

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.15"

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
SHEET_WORKORDERS     = "Workorders"         # history sheet
SHEET_ASSET_MASTER   = "Asset_Master"
SHEET_WO_MASTER      = "Workorders_Master"  # listing sheet with flags

# New sheets (optional but supported)
SHEET_WO_SERVICE     = "Workorders_Master_service"
SHEET_SERVICE_CANDIDATES = ["Service Report", "Service_Report", "ServiceReport"]
SHEET_METERS_MASTER  = "Meters_Master"
SHEET_READING_HISTORY= "Reading_History"
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
    if not data or len(data) < 100:
        raise RuntimeError("Downloaded file is unexpectedly small. Check repo/path/branch/token.")
    head = data[:128].lstrip()
    if head.startswith(b"{") or b"<html" in head.lower():
        raise RuntimeError("Got JSON/HTML instead of raw Excel. Check repo/path/branch/token.")
    return data

def get_xlsx_bytes(cfg: dict) -> bytes:
    xlsx_path = (cfg.get("settings", {}) or {}).get("xlsx_path")
    if xlsx_path:
        p = Path(xlsx_path)
        if not p.exists():
            raise FileNotFoundError(f"Local Excel not found: {xlsx_path}")
        return p.read_bytes()
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh:
        raise RuntimeError("No [github] secrets found. Configure repo/path/branch/token.")
    return download_bytes_from_github_file(
        repo=gh.get("repo"),
        path=gh.get("path"),
        branch=gh.get("branch", "main"),
        token=gh.get("token"),
    )

def get_data_last_updated() -> str | None:
    # Show latest XLSX commit time in ET
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh or not gh.get("repo") or not gh.get("path"):
        return None
    try:
        import requests
        from zoneinfo import ZoneInfo
        url = f"https://api.github.com/repos/{gh['repo']}/commits"
        params = {"path": gh["path"], "per_page": 1, "sha": gh.get("branch", "main")}
        headers = {"Accept": "application/vnd.github+json"}
        if gh.get("token"):
            headers["Authorization"] = f"token {gh['token']}"
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        iso = r.json()[0]["commit"]["committer"]["date"]  # UTC Z
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
    """Return df with columns renamed to canon keys when a known alias is found; keep others untouched."""
    low_to_orig = {str(c).strip().lower(): str(c) for c in df.columns}
    mapping = {}
    for key, aliases in canon.items():
        key_l = key.lower()
        # exact
        if key_l in low_to_orig:
            mapping[low_to_orig[key_l]] = key
            continue
        # alias hit
        for low, orig in low_to_orig.items():
            if low in aliases or low.replace("  ", " ") in aliases:
                mapping[orig] = key
                break
    return df.rename(columns=mapping)

# ---------- data loaders ----------
@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    """History sheet for Asset view."""
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

# ---- SERVICE: Workorders_Master_service (preferred) or Service Report (fallback)
SERVICE_CANON_WOMS = {
    "WO_ID": {"id","wo","workorder","work order","work order id","workorder id","wo id"},
    "Title": {"title"},
    "Service": {"service","procedure name","procedure","task","line item","step"},
    "Asset": {"asset"},
    "Location": {"location","ns location","location2"},
    "Date": {"completed on","performed on","date","completed date","service date"},
    "User": {"completed by","technician","assigned to","performed by","user"},
    "Notes": {"notes","description","comment","comments","details"},
    "Status": {"status"},
}
SERVICE_CANON_SR = {
    "WO_ID": {"workorder","wo","work order","work order id","id"},
    "Asset": {"asset"},
    "Location": {"location","ns location"},
    "Date": {"date","completed on","performed on"},
    "User": {"technician","completed by","performed by","assigned to","user"},
    "Service": {"procedure","procedure name","task","service","step"},
    "Notes": {"notes","description","comment"},
    "Status": {"status"},
}

@st.cache_data(show_spinner=False)
def load_service_df(xlsx_bytes: bytes) -> tuple[pd.DataFrame|None, str|None]:
    # Preferred: Workorders_Master_service
    try:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_WO_SERVICE, dtype=str, keep_default_na=False, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        df = _canonize_headers(df, SERVICE_CANON_WOMS)
        # normalize
        if "Date" in df.columns:
            df["Date"] = df["Date"].map(_norm_date_any)
        for c in [x for x in ["WO_ID","Title","Service","Asset","Location","User","Notes","Status"] if x in df.columns]:
            df[c] = df[c].astype(str).str.strip()
        return df, SHEET_WO_SERVICE
    except Exception:
        pass

    # Fallback: Service Report (any candidate name)
    for nm in SHEET_SERVICE_CANDIDATES:
        try:
            df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=nm, dtype=str, keep_default_na=False, engine="openpyxl")
            df.columns = [str(c).strip() for c in df.columns]
            df = _canonize_headers(df, SERVICE_CANON_SR)
            if "Date" in df.columns:
                df["Date"] = df["Date"].map(_norm_date_any)
            for c in [x for x in ["WO_ID","Service","Asset","Location","User","Notes","Status"] if x in df.columns]:
                df[c] = df[c].astype(str).str.strip()
            return df, nm
        except Exception:
            continue
    return None, None

# ---- METERS
METERS_CANON_MASTER = {
    "Meter": {"meter","meter name","name"},
    "Meter ID": {"meter id","meter_id","id"},
    "Asset": {"asset"},
    "Location": {"location","ns location","location2"},
    "Unit": {"unit","uom","units"},
    "Current Reading": {"current reading","reading","last reading","value"},
    "Current Date": {"current date","date","last date","reading date"},
}
METERS_CANON_HISTORY = {
    "Meter": {"meter","meter name","name"},
    "Asset": {"asset"},
    "Location": {"location","ns location","location2"},
    "Reading": {"reading","value"},
    "Date": {"date","reading date"},
    "User": {"user","entered by","taken by"},
}

@st.cache_data(show_spinner=False)
def load_meters_master_df(xlsx_bytes: bytes) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_METERS_MASTER, dtype=str, keep_default_na=False, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        df = _canonize_headers(df, METERS_CANON_MASTER)
        if "Current Date" in df.columns:
            df["Current Date"] = df["Current Date"].map(_norm_date_any)
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def load_meter_history_df(xlsx_bytes: bytes) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_READING_HISTORY, dtype=str, keep_default_na=False, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        df = _canonize_headers(df, METERS_CANON_HISTORY)
        if "Date" in df.columns:
            df["Date"] = df["Date"].map(_norm_date_any)
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def load_users_sheet(xlsx_bytes: bytes) -> list[str] | None:
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

    # Page choice in SIDEBAR (leave width for tables)
    page = st.sidebar.radio(
        "Page",
        ["🔎 Asset History", "📋 Work Orders", "🧾 Service Performed", "⛽ Meters"],
        index=1
    )

    # Load workbook bytes
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
            col = {
                "Open":"IsOpen","Overdue":"IsOverdue","Scheduled (Planning)":"IsScheduled","Completed":"IsCompleted","Old":"IsOld"
            }[view]
            return df_in[df_in[col]].copy()

        df_view = pick_view(df_scope)

        def present(cols: list[str]) -> list[str]:
            return [c for c in cols if c in df_view.columns]

        cols_all        = present(["ID","Title","Description","Asset","Created on","Planned Start Date","Due date","Started on","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_open       = present(["ID","Title","Description","Asset","Created on","Due date","Assigned to","Teams Assigned to","Location"])
        cols_overdue    = present(["ID","Title","Description","Asset","Due date","Assigned to","Teams Assigned to","Location"])
        cols_sched      = present(["ID","Title","Description","Asset","Planned Start Date","Due date","Assigned to","Teams Assigned to","Location"])
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

        # 7-day planner button
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

    # ========= Page: Service Performed =========
    elif page == "🧾 Service Performed":
        st.markdown("### Service Performed — per work order line")
        df_service, source_sheet = load_service_df(xlsx_bytes)
        if df_service is None:
            st.warning("No service-performed sheet found (looked for Workorders_Master_service or Service Report).")
            st.stop()

        # Restrict to allowed locations
        if "Location" in df_service.columns:
            df_service = df_service[df_service["Location"].isin(allowed_locations)].copy()

        # Filters
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        with c1:
            loc_values = sorted(df_service.get("Location", pd.Series([], dtype=str)).dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label], index=0)

        df_scope = df_service if (chosen_loc == loc_all_label or "Location" not in df_service.columns) else df_service[df_service["Location"] == chosen_loc].copy()

        with c2:
            assets = sorted([a for a in df_scope.get("Asset", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if a])
            sel_asset = st.selectbox("Asset (optional)", options=["— Any asset —"] + assets, index=0)

        with c3:
            users = sorted([u for u in df_scope.get("User", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if u])
            sel_user = st.selectbox("User (optional)", options=["— Any user —"] + users, index=0)

        with c4:
            # quick range
            range_opt = st.radio("Range", ["All", "Last 30 days", "This year"], horizontal=True)

        if sel_asset != "— Any asset —":
            df_scope = df_scope[df_scope["Asset"].astype(str).str.strip() == sel_asset].copy()

        if sel_user != "— Any user —":
            before = len(df_scope)
            df_scope = df_scope[df_scope["User"].astype(str).str.strip() == sel_user].copy()
            if before > 0 and df_scope.empty and chosen_loc != "« All my locations »":
                st.warning("User not found at this location (or no service records match).")
                st.dataframe(df_scope, use_container_width=True, hide_index=True)
                st.stop()

        if "Date" in df_scope.columns:
            dts = pd.to_datetime(df_scope["Date"], errors="coerce")
            today = pd.Timestamp.today().normalize()
            if range_opt == "Last 30 days":
                cutoff = today - pd.Timedelta(days=30)
                df_scope = df_scope[dts >= cutoff].copy()
            elif range_opt == "This year":
                jan1 = pd.Timestamp(year=today.year, month=1, day=1)
                df_scope = df_scope[dts >= jan1].copy()

        show_cols = [c for c in ["Date","WO_ID","Title","Service","Asset","User","Location","Notes","Status"] if c in df_scope.columns]
        df_scope = df_scope.sort_values(by=[c for c in ["Date","WO_ID","Asset","Service"] if c in df_scope.columns], na_position="last")

        st.caption(f"Source: {source_sheet}  •  Rows: {len(df_scope)}")
        st.dataframe(df_scope[show_cols] if show_cols else df_scope, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1,1,6])
        with c1:
            st.download_button(
                "⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(df_scope[show_cols] if show_cols else df_scope, sheet="Service_Performed"),
                file_name="Service_Performed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "⬇️ Word (.docx)",
                data=to_docx_bytes(df_scope[show_cols] if show_cols else df_scope, title="Service Performed"),
                file_name="Service_Performed.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

    # ========= Page: Meters =========
    else:
        st.markdown("### Meters")
        df_mm = load_meters_master_df(xlsx_bytes)
        df_hist = load_meter_history_df(xlsx_bytes)

        if df_mm is None and df_hist is None:
            st.warning("No meter sheets found (expected Meters_Master and/or Reading_History).")
            st.stop()

        # Restrict to allowed locations
        if df_mm is not None and "Location" in df_mm.columns:
            df_mm = df_mm[df_mm["Location"].isin(allowed_locations)].copy()
        if df_hist is not None and "Location" in df_hist.columns:
            df_hist = df_hist[df_hist["Location"].isin(allowed_locations)].copy()

        # Filters
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        # Build choice universe from either table
        loc_vals = set()
        asset_vals = set()
        meter_vals = set()
        if df_mm is not None:
            loc_vals.update(df_mm.get("Location", pd.Series([], dtype=str)))
            asset_vals.update(df_mm.get("Asset", pd.Series([], dtype=str)))
            meter_vals.update(df_mm.get("Meter", pd.Series([], dtype=str)))
        if df_hist is not None:
            loc_vals.update(df_hist.get("Location", pd.Series([], dtype=str)))
            asset_vals.update(df_hist.get("Asset", pd.Series([], dtype=str)))
            meter_vals.update(df_hist.get("Meter", pd.Series([], dtype=str)))
        loc_values = sorted([v for v in set(map(str, loc_vals)) if v and v in allowed_locations])
        assets_all = sorted([v for v in set(map(str, asset_vals)) if v])
        meters_all = sorted([v for v in set(map(str, meter_vals)) if v])

        with c1:
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values, index=0)
        with c2:
            sel_asset = st.selectbox("Asset", options=["— Any asset —"] + assets_all, index=0)
        with c3:
            sel_meter = st.selectbox("Meter", options=["— Any meter —"] + meters_all, index=0)
        with c4:
            range_opt = st.radio("History Range", ["All", "Last 90 days", "This year"], horizontal=True)

        # Apply filters
        if df_mm is not None:
            mm = df_mm.copy()
            if chosen_loc != loc_all_label and "Location" in mm.columns:
                mm = mm[mm["Location"] == chosen_loc]
            if sel_asset != "— Any asset —" and "Asset" in mm.columns:
                mm = mm[mm["Asset"] == sel_asset]
            if sel_meter != "— Any meter —" and "Meter" in mm.columns:
                mm = mm[mm["Meter"] == sel_meter]
        else:
            mm = pd.DataFrame()

        if df_hist is not None:
            hist = df_hist.copy()
            if chosen_loc != loc_all_label and "Location" in hist.columns:
                hist = hist[hist["Location"] == chosen_loc]
            if sel_asset != "— Any asset —" and "Asset" in hist.columns:
                hist = hist[hist["Asset"] == sel_asset]
            if sel_meter != "— Any meter —" and "Meter" in hist.columns:
                hist = hist[hist["Meter"] == sel_meter]
            if "Date" in hist.columns:
                dts = pd.to_datetime(hist["Date"], errors="coerce")
                today = pd.Timestamp.today().normalize()
                if range_opt == "Last 90 days":
                    cutoff = today - pd.Timedelta(days=90)
                    hist = hist[dts >= cutoff]
                elif range_opt == "This year":
                    jan1 = pd.Timestamp(year=today.year, month=1, day=1)
                    hist = hist[dts >= jan1]
        else:
            hist = pd.DataFrame()

        # Tabbed layout
        t_cur, t_hist = st.tabs(["Current snapshot", "Reading history & chart"])

        with t_cur:
            if mm.empty and not hist.empty:
                # synthesize "current" from history
                h = hist.copy()
                h["Date_dt"] = pd.to_datetime(h.get("Date"), errors="coerce")
                h["Reading_num"] = pd.to_numeric(h.get("Reading"), errors="coerce")
                idx = h.sort_values(["Asset","Meter","Date_dt"]).groupby(["Asset","Meter"], as_index=False).tail(1)
                show = idx.rename(columns={"Reading":"Current Reading","Date":"Current Date"})
                cols = [c for c in ["Meter","Asset","Current Reading","Current Date","Unit","Location","User"] if c in show.columns]
                st.dataframe(show[cols], use_container_width=True, hide_index=True)
                dl = show[cols]
            else:
                cols = [c for c in ["Meter","Asset","Current Reading","Current Date","Unit","Location"] if c in mm.columns]
                st.dataframe(mm[cols] if cols else mm, use_container_width=True, hide_index=True)
                dl = mm[cols] if cols else mm

            c1, c2, _ = st.columns([1,1,6])
            with c1:
                st.download_button(
                    "⬇️ Excel (.xlsx)", data=to_xlsx_bytes(dl, sheet="Meters_Current"),
                    file_name="Meters_Current.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with c2:
                st.download_button(
                    "⬇️ Word (.docx)", data=to_docx_bytes(dl, title="Meters — Current"),
                    file_name="Meters_Current.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )

        with t_hist:
            if hist.empty:
                st.info("No reading history in scope.")
            else:
                # Show table
                cols = [c for c in ["Date","Meter","Asset","Reading","User","Location"] if c in hist.columns]
                hist_sorted = hist.copy()
                if "Date" in hist_sorted.columns:
                    hist_sorted = hist_sorted.sort_values(by=["Meter","Asset","Date"], na_position="last")
                st.dataframe(hist_sorted[cols] if cols else hist_sorted, use_container_width=True, hide_index=True)

                # If a single meter is selected, draw a simple line chart
                if sel_meter != "— Any meter —":
                    try:
                        chart_df = hist_sorted[hist_sorted["Meter"] == sel_meter].copy()
                        chart_df["Date_dt"] = pd.to_datetime(chart_df["Date"], errors="coerce")
                        chart_df["Reading_num"] = pd.to_numeric(chart_df["Reading"], errors="coerce")
                        chart_df = chart_df.dropna(subset=["Date_dt","Reading_num"]).sort_values("Date_dt")
                        st.line_chart(chart_df.set_index("Date_dt")["Reading_num"])
                    except Exception:
                        pass

                c1, c2, _ = st.columns([1,1,6])
                with c1:
                    st.download_button(
                        "⬇️ Excel (.xlsx)", data=to_xlsx_bytes(hist_sorted[cols] if cols else hist_sorted, sheet="Meters_History"),
                        file_name="Meters_History.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                with c2:
                    st.download_button(
                        "⬇️ Word (.docx)", data=to_docx_bytes(hist_sorted[cols] if cols else hist_sorted, title="Meters — History"),
                        file_name="Meters_History.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )
