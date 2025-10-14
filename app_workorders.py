# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Workorders.xlsx from the same repo folder)
# Pages: Asset History • Work Orders • Service Report • Service History
# Privacy-safe by Location; Dates normalized where appropriate
# --------------------------------------------------------------

from __future__ import annotations
import io, re
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import yaml
from zipfile import BadZipFile

APP_VERSION = "2025.10.15g-local"

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

# Light CSS: slimmer selectboxes & hide Streamlit Cloud manage badge/footer
st.markdown("""
<style>
/* tighter selectboxes */
div[data-baseweb="select"] > div { min-height: 34px; }
label span { font-size: 0.85rem; }
/* Hide Streamlit Cloud 'Manage app' */
footer, #MainMenu {visibility: hidden;}
.stDeployButton, .stAppDeployButton {display:none !important;}
/* Badge container name changes over time; try to hide generically */
div[class*="viewerBadge"], a[title="Manage app"] {display:none !important;}
</style>
""", unsafe_allow_html=True)

# ---------- constants ----------
# Sheet names in your current workbook
SHEET_WORKORDERS   = "Workorders"                  # history sheet
SHEET_ASSET_MASTER = "Asset_Master"
SHEET_WO_MASTER    = "Workorders_Master"           # listing sheet with flags

# Service History sheet names (try these in order)
SHEET_WO_SERVICE_CANDS = [
    "Workorders_Master_Services",
    "Workorders_Master_service",
    "Workorders_Master_Service",
]

# Service Report sheet names (try these in order)
SHEET_SERVICE_CANDIDATES = ["Service Report", "Service_Report", "ServiceReport"]

SHEET_USERS_CANDIDATES   = ["Users", "Users]", "USERS", "users"]

REQUIRED_WO_COLS = [
    "WORKORDER","TITLE","STATUS","PO","P/N","QUANTITY RECEIVED",
    "Vendors","COMPLETED ON","ASSET","Location",
]
OPTIONAL_SORT_COL = "Sort"
ASSET_MASTER_COLS = ["Location","ASSET"]

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

def _norm_key(x: str) -> str:
    s = re.sub(r"[^0-9a-z]+", " ", str(x).lower())
    return re.sub(r"\s+", " ", s).strip()

def _norm_date_any(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%d-%b-%Y","%Y-%m-%d %H:%M:%S"):
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
        ws.autofilter(0, 0, max(0, len(df)), max(0, len(df.columns)-1))
        for i, col in enumerate(df.columns):
            width = 12 if df.empty else min(60, max(10, int(df[col].astype(str).str.len().quantile(0.9)) + 2))
            ws.set_column(i, i, width)
    return buf.getvalue()

def to_docx_bytes(df: pd.DataFrame, title: str) -> bytes:
    doc = Document()
    doc.styles["Normal"].font.name = "Calibri"
    doc.styles["Normal"].font.size = Pt(10)
    doc.add_heading(title, level=1)
    rows, cols = len(df)+1, len(df.columns)
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

# ---------- data bytes (local first) ----------
def get_xlsx_bytes(cfg: dict) -> bytes:
    # 1) use settings.xlsx_path if provided
    xlsx_path = (cfg.get("settings", {}) or {}).get("xlsx_path")
    here = Path(__file__).resolve().parent
    if not xlsx_path:
        # 2) default to 'Workorders.xlsx' next to this script
        xlsx_path = str(here / "Workorders.xlsx")
    p = Path(xlsx_path)
    if p.exists():
        return p.read_bytes()

    # 3) (Optional) fallback to GitHub secrets if local missing
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh:
        raise FileNotFoundError(f"Excel not found locally at: {p}. Provide settings.xlsx_path or place Workorders.xlsx next to app.")
    # GH download
    import requests
    def _headers(raw: bool = True):
        h = {"Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"}
        if gh.get("token"):
            h["Authorization"] = f"token {gh['token']}"
        return h
    url1 = f"https://api.github.com/repos/{gh.get('repo')}/contents/{gh.get('path')}?ref={gh.get('branch','main')}"
    r1 = requests.get(url1, headers=_headers(raw=True), timeout=30)
    if r1.status_code == 200:
        data = r1.content
    else:
        url2 = f"https://raw.githubusercontent.com/{gh.get('repo')}/{gh.get('branch','main')}/{gh.get('path')}"
        r2 = requests.get(url2, headers=_headers(raw=True), timeout=30)
        if r2.status_code != 200:
            snippet1 = (r1.text or "")[:200]
            snippet2 = (r2.text or "")[:200]
            raise RuntimeError(f"GitHub download failed.\nContents API ({r1.status_code}): {snippet1}\nRaw URL ({r2.status_code}): {snippet2}")
        data = r2.content
    if not data or len(data) < 100:
        raise RuntimeError("Downloaded file is unexpectedly small. Check repo/path/branch/token.")
    head = data[:128].lstrip()
    if head.startswith(b"{") or b"<html" in head.lower():
        raise RuntimeError("Got JSON/HTML instead of raw Excel. Check repo/path/branch/token.")
    return data

def get_data_last_updated_local(path: Path) -> str | None:
    try:
        ts = datetime.fromtimestamp(path.stat().st_mtime)
        return ts.strftime("Data last updated (local): %Y-%m-%d %H:%M")
    except Exception:
        return None

# ---------- loaders ----------
@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED_WO_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")
    cols = REQUIRED_WO_COLS[:]
    if OPTIONAL_SORT_COL in df.columns:
        cols += [OPTIONAL_SORT_COL]
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

# Service Report loader (raw only; we’ll format specific columns for display later)
@st.cache_data(show_spinner=False)
def load_service_report_df(xlsx_bytes: bytes):
    last_err = None
    for nm in SHEET_SERVICE_CANDIDATES:
        try:
            raw = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=nm, dtype=str, keep_default_na=False, engine="openpyxl")
            raw.columns = [str(c).strip() for c in raw.columns]
            return raw, nm
        except Exception as e:
            last_err = e
            continue
    return None, f"{last_err}" if last_err else None

# Service History loader (Location2)
@st.cache_data(show_spinner=False)
def load_service_history_df(xlsx_bytes: bytes):
    last_err = None
    for nm in SHEET_WO_SERVICE_CANDS:
        try:
            df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=nm, dtype=str, keep_default_na=False, engine="openpyxl")
            df.columns = [str(c).strip() for c in df.columns]
            # Normalize key columns (your provided header set)
            rename = {
                "ID":"WO_ID",
                "Completed on":"Date",
                "Service Type":"Service",
                "Location2":"Location2",
                "MReading":"MReading",
                "MHours":"MHours",
            }
            for k,v in rename.items():
                if k in df.columns:
                    df = df.rename(columns={k:v})
            if "Date" in df.columns:
                df["Date"] = df["Date"].map(_norm_date_any)
            for c in [x for x in ["WO_ID","Title","Service","Asset","User","Notes","Status","Location2","MReading","MHours"] if x in df.columns]:
                df[c] = df[c].astype(str).str.strip()
            keep = [c for c in ["Date","WO_ID","Title","Service","MReading","MHours","Asset","Location2"] if c in df.columns]
            df = df[keep].copy() if keep else df
            return df, nm
        except Exception as e:
            last_err = e
            continue
    return None, f"{last_err}" if last_err else None

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

    # Load workbook bytes (local first)
    try:
        xlsx_bytes = get_xlsx_bytes(cfg)
        # show local updated timestamp if local path exists
        p = Path((cfg.get("settings", {}) or {}).get("xlsx_path") or (Path(__file__).resolve().parent / "Workorders.xlsx"))
        if p.exists():
            st.sidebar.caption(get_data_last_updated_local(p) or "")
    except Exception as e:
        st.error(f"Could not load Excel: {e}")
        st.stop()

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    page = st.sidebar.radio(
        "Page",
        ["🔎 Asset History", "📋 Work Orders", "🧾 Service Report", "📚 Service History"],
        index=1
    )

    # Access control: Locations
    try:
        df_am = load_asset_master_df(xlsx_bytes, SHEET_ASSET_MASTER)
    except BadZipFile:
        st.error("The downloaded file isn’t a valid .xlsx.")
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
    allowed_norms = {_norm_key(x) for x in allowed_locations}

    # ========= Asset History =========
    if page == "🔎 Asset History":
        st.markdown("### Asset History")
        c1, c2 = st.columns([2, 3])
        with c1:
            chosen_loc = st.selectbox("Location", options=sorted(allowed_locations), index=0, key="ah_loc")
        with c2:
            assets_for_loc = sorted(df_am.loc[df_am["Location"] == chosen_loc, "ASSET"].dropna().unique().tolist())
            chosen_asset = st.selectbox("Asset", options=assets_for_loc, index=0 if assets_for_loc else None, key="ah_asset")

        if not assets_for_loc:
            st.info("No assets for this Location.")
            st.stop()

        try:
            df_all = load_workorders_df(xlsx_bytes, SHEET_WORKORDERS)
        except Exception as e:
            st.error(f"Failed to read Workorders (history): {e}")
            st.stop()

        df = df_all[(df_all["Location"] == chosen_loc) & (df_all["ASSET"] == chosen_asset)].copy()

        # Remove negative/zero part transactions
        if "QUANTITY RECEIVED" in df.columns and "P/N" in df.columns:
            qnum = pd.to_numeric(df["QUANTITY RECEIVED"], errors="coerce")
            is_part = df["P/N"].astype(str).str.strip().ne("")
            df = df[~(is_part & qnum.notna() & (qnum <= 0))].copy()

        df["__row"] = range(len(df))
        if OPTIONAL_SORT_COL in df.columns:
            df["__sort_key"] = pd.to_numeric(df[OPTIONAL_SORT_COL], errors="coerce").fillna(1).astype(int)
        else:
            df["__sort_key"] = 1
        df.sort_values(by=["WORKORDER","__sort_key","__row"], ascending=[True, True, True], inplace=True)
        df.loc[df["__sort_key"].isin([2, 3]), "WORKORDER"] = ""

        drop_cols = ["__row","__sort_key", OPTIONAL_SORT_COL]
        df_out = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

        st.dataframe(df_out, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(df_out, sheet="Workorders"),
                               file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.xlsx".replace(" ","_"),
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with c2:
            st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(df_out, title=f"Work Orders — {chosen_loc} — {chosen_asset}"),
                               file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.docx".replace(" ","_"),
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        st.stop()

    # ========= Work Orders =========
    if page == "📋 Work Orders":
        st.markdown("### Work Orders — Filtered Views (flags from workbook)")

        try:
            df_master = load_wo_master_df(xlsx_bytes, SHEET_WO_MASTER)
        except Exception as e:
            st.error(f"Failed to read '{SHEET_WO_MASTER}': {e}")
            st.stop()

        opt_users = load_users_sheet(xlsx_bytes)  # <-- fixed (pass real bytes)

        df_master = df_master[df_master["Location"].isin(allowed_locations)].copy()
        total_in_scope = len(df_master)

        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        with c1:
            loc_values = sorted(df_master["Location"].dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values, index=0, key="wo_loc")
        df_scope = df_master if chosen_loc == loc_all_label else df_master[df_master["Location"] == chosen_loc].copy()

        with c2:
            if opt_users is not None:
                user_choices = ["— Any user —"] + opt_users
            else:
                derived_users = sorted([u for u in df_scope.get("Assigned to", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if u])
                user_choices = ["— Any user —"] + derived_users
            sel_user = st.selectbox("Assigned user", options=user_choices, index=0, key="wo_user")

        with c3:
            raw_teams = df_scope.get("Teams Assigned to", pd.Series([], dtype=str)).fillna("").astype(str)
            token_set = set()
            for v in raw_teams:
                for t in re.split(r"[;,]", v):
                    t = t.strip()
                    if t:
                        token_set.add(t)
            team_opts = ["— Any team —"] + sorted(token_set)
            sel_team = st.selectbox("Team", options=team_opts, index=0, key="wo_team")

        with c4:
            view = st.radio("View", ["All","Open","Overdue","Scheduled (Planning)","Completed","Old"], horizontal=True,
                            index=1 if "IsOpen" in df_scope.columns else 0, key="wo_view")

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

        cols_all       = present(["ID","Title","Description","Asset","Created on","Planned Start Date","Due date","Started on","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_open      = present(["ID","Title","Description","Asset","Created on","Due date","Assigned to","Teams Assigned to","Location"])
        cols_overdue   = present(["ID","Title","Description","Asset","Due date","Assigned to","Teams Assigned to","Location"])
        cols_sched     = present(["ID","Title","Description","Asset","Planned Start Date","Due date","Assigned to","Teams Assigned to","Location"])
        cols_completed = present(["ID","Title","Description","Asset","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_old       = present(["ID","Title","Description","Asset","Created on","Due date","Completed on","Assigned to","Teams Assigned to","Location"])

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
            st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(df_view[use_cols], sheet="WorkOrders"),
                               file_name=f"WorkOrders_{view.replace(' ','_')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with c2:
            st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(df_view[use_cols], title=f"Work Orders — {view}"),
                               file_name=f"WorkOrders_{view.replace(' ','_')}.docx",
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        st.stop()

    # ========= Service Report =========
    if page == "🧾 Service Report":
        st.markdown("### Service Report")
        raw_sr, source_sheet_or_err = load_service_report_df(xlsx_bytes)
        if raw_sr is None:
            st.warning(f"No 'Service Report' sheet found. Last error: {source_sheet_or_err}")
            st.stop()

        # Location filter (shorter)
        # Allow matching on Location or Location2 variants
        possible_loc_cols = [c for c in raw_sr.columns if c.strip().lower() in {"location","ns location","location2"}]
        loc_col = possible_loc_cols[0] if possible_loc_cols else None

        if loc_col:
            loc_values_all = raw_sr[loc_col].astype(str)
            # in-scope only
            loc_values = sorted([v for v in loc_values_all.unique().tolist() if _norm_key(v) in allowed_norms])
        else:
            loc_values = sorted(allowed_locations)

        c1, = st.columns([3])
        loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
        chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label],
                                  index=0, key="sr_loc", label_visibility="collapsed")
        st.caption("Location")

        if loc_col and chosen_loc != loc_all_label:
            mask = loc_values_all.map(_norm_key) == _norm_key(chosen_loc)
            raw_show = raw_sr[mask].copy()
        else:
            raw_show = raw_sr.copy()

        # ---- Tabs
        t_report, t_due, t_over = st.tabs(["Report", "Coming Due", "Overdue"])

        # Helper: return date-only formatted copy of a column if it truly parses as datetime
        def _date_only_if_datetime(df: pd.DataFrame, col: str) -> pd.Series:
            if col not in df.columns:
                return df.get(col)
            s = df[col].astype(str).str.strip()
            parsed = pd.to_datetime(s, errors="coerce")
            if parsed.notna().any():
                out = s.copy()
                out.loc[parsed.notna()] = parsed.dropna().dt.strftime("%Y-%m-%d")
                return out
            return df[col]

        # --- Report (as-is but hide Schedule/Today and force Last Reading to date-only if it's datetime)
        with t_report:
            show = raw_show.copy()
            # Last Reading: only change if it looks like a datetime column
            if "Last Reading" in show.columns:
                show["Last Reading"] = _date_only_if_datetime(show, "Last Reading")
            # Hide Schedule and Today
            show = show.drop(columns=[c for c in ["Schedule","Today"] if c in show.columns], errors="ignore")
            st.caption(f"Source: {source_sheet_or_err} • Rows: {len(show)} • Only Location filter applied.")
            st.dataframe(show, use_container_width=True, hide_index=True)

            c1, c2, _ = st.columns([1,1,6])
            with c1:
                st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(show, sheet="Service_Report"),
                                   file_name="Service_Report.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(show, title="Service Report — As Is"),
                                   file_name="Service_Report.docx",
                                   mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        # ---- Coming Due / Overdue logic based on Schedule/Remaining/Meter Type
        def _due_frames(df_in: pd.DataFrame):
            if df_in is None or df_in.empty:
                return pd.DataFrame(), pd.DataFrame()
            df = df_in.copy()

            # numeric helpers
            def _num(col):
                if col not in df.columns: return pd.Series([pd.NA]*len(df))
                return pd.to_numeric(df[col].astype(str).str.replace("%","", regex=False), errors="coerce")

            sched = _num("Schedule")
            remaining = _num("Remaining")
            pct = pd.Series([pd.NA]*len(df))  # not used now, but left for future

            meter_type = df["Meter Type"].astype(str).str.lower() if "Meter Type" in df.columns else pd.Series([""]*len(df))
            thr = meter_type.map(lambda x: 0.05 if "mile" in x else 0.10)

            coming_due_mask = (sched.notna() & (sched > 0) & remaining.notna() & (remaining >= 0) &
                               (remaining <= (sched * thr)))
            coming_due = df[coming_due_mask].copy()

            # Overdue if Remaining < 0 (no Due Date column in current headers)
            overdue_mask = remaining.notna() & (remaining < 0)
            overdue = df[overdue_mask].copy()

            return coming_due, overdue

        coming_due_df, overdue_df = _due_frames(raw_show)

        def present_due(df: pd.DataFrame, include_mreading: bool = False):
            base = ["Location","Name","Last Reading","Date","Last Service Type",
                    "Date of Last service","Hours/Miles at Last Service","Next Service Type",
                    "Next Service","Remaining","Meter Type","Schedule"]
            if include_mreading and "MReading" in df.columns:
                # we'll insert MReading right after Schedule if present
                cols = []
                for c in base:
                    cols.append(c) if c in df.columns else None
                    if c == "Schedule" and "MReading" in df.columns:
                        cols.append("MReading")
                return df[cols] if cols else df
            else:
                cols = [c for c in base if c in df.columns]
                return df[cols] if cols else df

        with t_due:
            st.caption("Coming Due = Remaining ≤ 10% of Schedule (or ≤ 5% if Meter Type contains 'miles').")
            if coming_due_df.empty:
                st.info("No items are coming due.")
            else:
                show = present_due(coming_due_df, include_mreading=False)
                st.dataframe(show.sort_values(by=[c for c in ["Next Service","Date","Date of Last service","Remaining"] if c in show.columns], na_position="last"),
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
            st.caption("Overdue = Remaining < 0.")
            if overdue_df.empty:
                st.info("No overdue items found.")
            else:
                # If 'MReading' exists in this sheet, place after Schedule; otherwise, just show the normal set
                show = present_due(overdue_df, include_mreading=True)
                st.dataframe(show.sort_values(by=[c for c in ["Next Service","Remaining"] if c in show.columns], na_position="last"),
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
        st.stop()

    # ========= Service History =========
    if page == "📚 Service History":
        st.markdown("### Service History")

        df_hist, used_sheet_or_err = load_service_history_df(xlsx_bytes)
        if df_hist is None or df_hist.empty:
            msg = used_sheet_or_err or "unknown error"
            st.warning(f"No Service History data found. Tried: {SHEET_WO_SERVICE_CANDS}. Last error: {msg}")
            st.stop()

        # Restrict to allowed locations via Location2
        if "Location2" in df_hist.columns:
            df_hist["__LocNorm"] = df_hist["Location2"].map(_norm_key)
            df_hist = df_hist[df_hist["__LocNorm"].isin(allowed_norms)].copy()

        # Filters: Location2 + Asset
        c1, c2 = st.columns([2, 3])
        with c1:
            if "Location2" in df_hist.columns:
                loc_values = sorted(df_hist["Location2"].dropna().unique().tolist())
            else:
                loc_values = []
            loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label],
                                      index=0, key="sh_loc", label_visibility="collapsed")
            st.caption("Location")

        if chosen_loc != loc_all_label and "Location2" in df_hist.columns:
            scope = df_hist[_norm_key(df_hist["Location2"]) == _norm_key(chosen_loc)].copy()
        else:
            scope = df_hist.copy()

        with c2:
            assets = sorted([a for a in scope.get("Asset", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if a])
            sel_asset = st.selectbox("Asset", options=assets, index=0 if assets else None, key="sh_asset")

        if not assets:
            st.info("No assets available in this Location.")
            st.stop()

        scope = scope[scope["Asset"] == sel_asset] if "Asset" in scope.columns else scope

        if "Date" in scope.columns:
            scope = scope.copy()
            scope["__Date_dt"] = pd.to_datetime(scope["Date"], errors="coerce")
            scope["Date"] = scope["__Date_dt"].dt.strftime("%Y-%m-%d").where(scope["__Date_dt"].notna(), scope["Date"])
            scope = scope.sort_values(by="__Date_dt", ascending=False, na_position="last").drop(columns="__Date_dt")

        # Ensure MReading displayed after Service
        # Build column order dynamically
        base_cols = []
        for c in ["Date","WO_ID","Title","Service","MReading","MHours","Asset","Location2"]:
            if c in scope.columns:
                base_cols.append(c)
        show_df = scope[base_cols] if base_cols else scope

        st.caption(f"Sheet used: {used_sheet_or_err} • Rows: {len(scope)}")
        st.dataframe(show_df, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1,1,6])
        with c1:
            st.download_button(
                "⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(show_df, sheet="Service_History"),
                file_name=f"Service_History_{sel_asset.replace(' ','_')}.xlsx" if assets else "Service_History.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                "⬇️ Word (.docx)",
                data=to_docx_bytes(show_df, title=f"Service History — {sel_asset}" if assets else "Service History"),
                file_name=f"Service_History_{sel_asset.replace(' ','_')}.docx" if assets else "Service_History.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        st.stop()

