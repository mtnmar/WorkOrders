# app_workorders_excel.py
# --------------------------------------------------------------
# SPF Work Orders portal (reads Excel directly to keep original order)
# - Login (streamlit-authenticator)
# - Authorize by Location
# - Choose Location -> Asset from Asset_Master (unique lists)
# - Table shows history for that asset in EXACT Excel row order
# - COMPLETED ON displayed as date-only (no reordering)
# - Downloads: Excel (.xlsx) and Word (.docx)
#
# requirements.txt (minimum):
#   streamlit>=1.37
#   streamlit-authenticator==0.2.3
#   pandas>=2.0
#   openpyxl>=3.1
#   xlsxwriter>=3.2
#   python-docx>=1.1
#   pyyaml>=6.0
#   requests>=2.31

from __future__ import annotations
import io, textwrap
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.12"
SHEET_WORKORDERS = "Workorders"
SHEET_ASSET_MASTER = "Asset_Master"

# ---------- deps ----------
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator not installed. Add to requirements.txt")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.error("python-docx not installed. Add to requirements.txt")
    st.stop()

st.set_page_config(page_title="SPF Work Orders", page_icon="🛠️", layout="wide")

# ---------- config template ----------
CONFIG_TEMPLATE_YAML = """
credentials:
  usernames:
    demo:
      name: Demo User
      email: demo@example.com
      password: "$2b$12$y2J3Y0rRrJ3fA76h2o//mO6F1T0m3b1vS7QhQ4bW5iX9b5b5b5b5e"

cookie:
  name: spf_wo_portal
  key: super_secret_key_wo
  expiry_days: 7

access:
  admin_usernames: [demo]
  user_locations:
    demo: ['*']

settings:
  db_path: ""    # unused here (we read Excel directly via secrets->github)
"""

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
    return yaml.safe_load(CONFIG_TEMPLATE_YAML)

@st.cache_data(show_spinner=False)
def download_excel_from_github(repo: str, path: str, branch: str, token: str|None) -> bytes:
    import requests
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    return r.content

@st.cache_data(show_spinner=False)
def get_data_last_updated_from_github(repo: str, path: str, branch: str, token: str|None) -> str|None:
    import requests
    url = f"https://api.github.com/repos/{repo}/commits"
    params = {"path": path, "per_page": 1, "sha": branch}
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        iso = r.json()[0]["commit"]["committer"]["date"]
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("Data last updated: %Y-%m-%d %H:%M UTC")
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    """Read Workorders as strings; KEEP ORIGINAL ROW ORDER. No sorting."""
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet_name,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]
    df["_EXCEL_ORDER"] = range(1, len(df) + 1)  # stabilizer if ever needed
    return df

@st.cache_data(show_spinner=False)
def load_asset_master_df(xlsx_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    """Read Asset_Master; expect columns Location + ASSET (or Asset)."""
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet_name,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]
    # Normalize column names
    col_map = {c.casefold(): c for c in df.columns}
    loc_col = col_map.get("location", None)
    asset_col = col_map.get("asset", None) or col_map.get("asset ", None) or col_map.get("as set", None)
    if asset_col is None and "ASSET" in df.columns:
        asset_col = "ASSET"
    if asset_col is None and "Asset" in df.columns:
        asset_col = "Asset"
    if loc_col is None:  # try exact
        loc_col = "Location"
    if asset_col is None:
        asset_col = "ASSET"

    missing = [c for c in (loc_col, asset_col) if c not in df.columns]
    if missing:
        raise ValueError(f"Asset_Master is missing required column(s): {missing}. Found: {list(df.columns)}")

    # Keep only needed columns in original row order; ensure names Location/ASSET
    df = df[[loc_col, asset_col]].copy()
    if loc_col != "Location":
        df.rename(columns={loc_col: "Location"}, inplace=True)
    if asset_col != "ASSET":
        df.rename(columns={asset_col: "ASSET"}, inplace=True)

    # Strip whitespace in cells
    for c in ("Location", "ASSET"):
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)

    return df

def unique_first_seen(series: pd.Series) -> list[str]:
    seen = set()
    out = []
    for v in series:
        if v not in seen and v not in ("", None):
            seen.add(v)
            out.append(v)
    return out

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        df.to_excel(w, index=False, sheet_name=sheet)
        ws = w.sheets[sheet]
        ws.autofilter(0, 0, max(0, len(df)), max(0, len(df.columns) - 1))
        for i, col in enumerate(df.columns):
            lens = df[col].astype(str).str.len()
            qv = lens.quantile(0.9) if not lens.empty else 10
            qv = 10 if pd.isna(qv) else qv
            ws.set_column(i, i, min(60, max(10, int(qv) + 2)))
    return buf.getvalue()

def to_docx_bytes(df: pd.DataFrame, title: str) -> bytes:
    doc = Document()
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)
    doc.add_heading(title, level=1)
    rows, cols = len(df) + 1, len(df.columns)
    tbl = doc.add_table(rows=rows, cols=cols)
    tbl.style = 'Table Grid'
    for j, c in enumerate(df.columns):
        tbl.cell(0, j).text = str(c)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        for j, c in enumerate(df.columns):
            v = '' if pd.isna(r[c]) else str(r[c])
            tbl.cell(i, j).text = v
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()

# ---------- App ----------
st.sidebar.caption(f"SPF Work Orders — v{APP_VERSION}")
cfg = load_config()

# Auth
cookie_cfg = cfg.get('cookie', {})
auth = stauth.Authenticate(
    cfg.get('credentials', {}),
    cookie_cfg.get('name', 'spf_wo_portal'),
    cookie_cfg.get('key',  'super_secret_key_wo'),
    cookie_cfg.get('expiry_days', 7),
)
name, auth_status, username = auth.login("Login", "main")

if auth_status is False:
    st.error('Username/password is incorrect')
elif auth_status is None:
    st.info('Please log in.')
else:
    auth.logout('Logout', 'sidebar')
    st.sidebar.success(f"Logged in as {name}")

    # GitHub file info
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else {}
    repo   = gh.get('repo')
    path   = gh.get('path')      # must be "Workorders.xlsx"
    branch = gh.get('branch', 'main')
    token  = gh.get('token')

    if not (repo and path):
        st.error("Secrets [github] must include repo, path (Workorders.xlsx).")
        st.stop()

    # Download Excel once
    xlsx_bytes = download_excel_from_github(repo, path, branch, token)
    last_updated = get_data_last_updated_from_github(repo, path, branch, token)
    if last_updated:
        st.sidebar.caption(last_updated)

    # Load sheets (keep original row orders)
    df_all = load_workorders_df(xlsx_bytes, SHEET_WORKORDERS)
    df_master = load_asset_master_df(xlsx_bytes, SHEET_ASSET_MASTER)

    # Authorization by Location (case-insensitive)
    username_ci = str(username).casefold()
    admin_users_raw = (cfg.get('access', {}).get('admin_usernames', []) or [])
    admin_users_ci = {str(u).casefold() for u in admin_users_raw}
    is_admin = username_ci in admin_users_ci

    ul_raw = (cfg.get('access', {}).get('user_locations', {}) or {})
    ul_ci_map = {str(k).casefold(): v for k, v in ul_raw.items()}
    allowed_cfg = ul_ci_map.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    def norm(s: str) -> str:
        return " ".join(str(s).strip().split()).casefold()

    # Build unique Location list from Asset_Master (first-seen order)
    locs_master = unique_first_seen(df_master["Location"]) if "Location" in df_master.columns else []
    if is_admin or any(str(a).strip() == "*" for a in allowed_cfg):
        allowed_locations = locs_master
    else:
        allowed_norm = {norm(a) for a in allowed_cfg}
        allowed_locations = [L for L in locs_master if norm(L) in allowed_norm]

    if not allowed_locations:
        st.error("No locations configured for your account. Ask an admin to update your access.")
        with st.expander("Locations in Asset_Master"):
            st.write(locs_master)
        st.stop()

    # UI: Location -> Asset (both required), from Asset_Master only
    loc_choice = st.sidebar.selectbox("Choose Location", options=["— Choose location —"] + allowed_locations, index=0)
    if loc_choice == "— Choose location —":
        st.info("Select a Location on the left.")
        st.stop()

    df_assets_for_loc = df_master[df_master["Location"] == loc_choice]
    assets_for_loc = unique_first_seen(df_assets_for_loc["ASSET"]) if "ASSET" in df_assets_for_loc.columns else []
    if not assets_for_loc:
        st.warning("No assets listed for that Location in Asset_Master.")
        st.stop()

    asset_choice = st.sidebar.selectbox("Choose Asset", options=["— Choose asset —"] + assets_for_loc, index=0)
    if asset_choice == "— Choose asset —":
        st.info("Select an Asset on the left.")
        st.stop()

    # Optional search (does NOT change order)
    search = st.sidebar.text_input('Search Title / PO / P/N (optional)')

    # Filter rows (preserve Excel order) using Workorders sheet
    mask = (df_all.get("Location", "") == loc_choice) & (df_all.get("ASSET", "") == asset_choice)
    if search:
        like = str(search).strip().casefold()
        def match_any(row):
            return any(like in str(row.get(col, "")).casefold() for col in ("TITLE","PO","P/N"))
        mask = mask & df_all.apply(match_any, axis=1)

    df = df_all[mask].copy()  # keeps original row order

    # Display tweak: COMPLETED ON => date-only string
    if "COMPLETED ON" in df.columns:
        dt = pd.to_datetime(df["COMPLETED ON"], errors="coerce")
        df["COMPLETED ON"] = dt.dt.date.astype(str).where(dt.notna(), "")

    # Hide helper column
    if "_EXCEL_ORDER" in df.columns:
        df.drop(columns=["_EXCEL_ORDER"], inplace=True)

    title_txt = f"{loc_choice} — {asset_choice}"
    st.markdown(f"### Work Orders — {title_txt}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads (use displayed df)
    def _xlsx_bytes(dfx): return to_xlsx_bytes(dfx, sheet="WorkOrders")
    def _docx_bytes(dfx): return to_docx_bytes(dfx, title=f"Work Orders — {title_txt}")

    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=_xlsx_bytes(df),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.xlsx".replace(" ", "_"),
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=_docx_bytes(df),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.docx".replace(" ", "_"),
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # Admins: show config template
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')


