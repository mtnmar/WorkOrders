# app_workorders.py
# --------------------------------------------------------------
# Work Orders portal
# - Login (streamlit-authenticator)
# - Authorize & filter by Location
# - Location -> Asset dropdown (assets are unique within chosen location)
# - Preserves WO -> PO -> Transaction order via (WORKORDER, Sort)
# - "Completed Date" shown as date-only (YYYY-MM-DD)
# - Downloads: Excel (.xlsx) and Word (.docx)
#
# Secrets expected (recommended TOML form):
# [app_config.credentials.usernames.YOURUSER]
# name = "Your Name"
# email = "you@example.com"
# password = "bcrypt_hash"
#
# [app_config.access]
# admin_usernames = ["brad"]
#
# [app_config.access.user_locations]
# brad = ["*"]
# dlauer = ["110 - Deckers Creek Limestone", "240 - Buckeye Stone"]
#
# [github]  # optional, for Streamlit Cloud pull of DB
# repo = "mtnmar/spf-data"
# path = "maintainx_workorders.db"
# branch = "main"
# token = "ghp_..."
#
# requirements.txt (minimal):
# streamlit>=1.37
# streamlit-authenticator==0.2.3
# pandas>=2.0
# xlsxwriter>=3.2
# python-docx>=1.1
# pyyaml>=6.0
# requests>=2.31

from __future__ import annotations
import os, io, sqlite3, textwrap
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.12"
DEFAULT_DB = "maintainx_workorders.db"  # local fallback
TABLE = "workorders"                    # table name in the DB

# ---- deps ----
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

st.set_page_config(page_title="Work Orders", page_icon="🧰", layout="wide")

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
    cfg_file = Path(__file__).resolve().parent / "app_config.yaml"
    if cfg_file.exists():
        try:
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    # tiny fallback template
    return {
        "cookie": {"name":"wo_portal","key":"change_me","expiry_days":7},
        "access": {"admin_usernames": [], "user_locations": {}},
        "settings": {"db_path": ""}
    }

def resolve_db_path(cfg: dict) -> str:
    ydb = (cfg.get("settings") or {}).get("db_path")
    if ydb:
        return ydb
    env = os.environ.get("SPF_DB_PATH")
    if env:
        return env
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh and gh.get('repo') and gh.get('path'):
        try:
            import requests, tempfile
            url = f"https://api.github.com/repos/{gh['repo']}/contents/{gh['path']}?ref={gh.get('branch','main')}"
            headers = {"Accept": "application/vnd.github.v3.raw"}
            if gh.get("token"):
                headers["Authorization"] = f"token {gh['token']}"
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"GitHub API {r.status_code}: {r.text[:200]}")
            tmpdir = Path(tempfile.gettempdir()) / "spf_wo_cache"
            tmpdir.mkdir(parents=True, exist_ok=True)
            out = tmpdir / "maintainx_workorders.db"
            out.write_bytes(r.content)
            return str(out)
        except Exception as e:
            st.error(f"GitHub DB fetch failed: {e}")
    return DEFAULT_DB

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def table_columns(db_path: str, table: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def pick_col(cols: list[str], candidates: list[str]) -> str | None:
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None

def to_xlsx_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    import xlsxwriter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
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

def last_updated_label(db_path: str) -> str | None:
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh and gh.get('repo') and gh.get('path'):
        try:
            import requests
            url = f"https://api.github.com/repos/{gh['repo']}/commits"
            params = {"path": gh["path"], "per_page": 1, "sha": gh.get("branch","main")}
            headers = {"Accept": "application/vnd.github+json"}
            if gh.get("token"):
                headers["Authorization"] = f"token {gh['token']}"
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            iso = r.json()[0]["commit"]["committer"]["date"]
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.strftime("Data last updated: %Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    try:
        ts = Path(db_path).stat().st_mtime
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("Data last updated: %Y-%m-%d %H:%M UTC")
    except Exception:
        return None

# ---------- App ----------
cfg = load_config()
cookie_cfg = cfg.get('cookie', {}) or {"name":"wo_portal","key":"change_me","expiry_days":7}

auth = stauth.Authenticate(
    cfg.get('credentials', {}),
    cookie_cfg.get('name', 'wo_portal'),
    cookie_cfg.get('key',  'change_me'),
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

    db_path = resolve_db_path(cfg)

    # Sidebar: only show "last updated"
    lbl = last_updated_label(db_path)
    if lbl:
        st.sidebar.caption(lbl)

    # Discover columns in DB so we can adapt to headers
    cols = table_columns(db_path, TABLE)
    if not cols:
        st.error(f"No columns found in table '{TABLE}'.")
        st.stop()

    # Find key columns by common names
    location_col   = pick_col(cols, ["Location", "Company", "Site"])
    asset_col      = pick_col(cols, ["Asset", "ASSET", "Equipment", "Machine"])
    wo_col         = pick_col(cols, ["WORKORDER", "Work Order", "WO"])
    sort_col       = pick_col(cols, ["Sort", "SORT", "Order"])
    completed_col  = pick_col(cols, ["Completed Date", "COMPLETED DATE", "Date Completed", "Completed"])

    if not location_col or not asset_col or not wo_col:
        st.error(f"Missing required columns. Need at least Location='{location_col}', Asset='{asset_col}', Work Order='{wo_col}'.")
        with st.expander("Columns present in table"):
            st.write(cols)
        st.stop()

    # Authorization by location (case-insensitive username)
    username_ci = str(username).casefold()
    admin_users = [str(u) for u in (cfg.get('access', {}).get('admin_usernames', []) or [])]
    is_admin = username_ci in {u.casefold() for u in admin_users}

    # Build allowed location set for this user
    uc_locs = (cfg.get('access', {}).get('user_locations', {}) or {})
    # Case-insensitive username lookup
    uc_ci = {str(k).casefold(): v for k, v in uc_locs.items()}
    allowed_cfg = uc_ci.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    # All locations present in table
    all_locs_df = q(f'SELECT DISTINCT [{location_col}] AS L FROM [{TABLE}] WHERE [{location_col}] IS NOT NULL ORDER BY 1', db_path=db_path)
    all_locs = [str(x) for x in all_locs_df["L"].dropna().tolist()]

    if is_admin or any(str(a).strip() == "*" for a in allowed_cfg):
        allowed_set = set(all_locs)
    else:
        # Only those present in DB
        normalized_all = {s.strip().casefold(): s for s in all_locs}
        allowed_norm = {" ".join(a.strip().split()).casefold() for a in allowed_cfg}
        allowed_set = {normalized_all[n] for n in allowed_norm if n in normalized_all}

        if not allowed_set:
            # Fallback to showing all (prevents total lockout if mapping is stale)
            allowed_set = set(all_locs)

    # 1) Choose Location (required)
    loc_options = ["— Choose location —"] + sorted(allowed_set)
    loc_choice = st.sidebar.selectbox("Location", options=loc_options, index=0)
    if loc_choice == "— Choose location —":
        st.info("Select your Location on the left.")
        st.stop()

    # 2) Asset dropdown (unique within chosen location; searchable)
    assets_df = q(
        f'SELECT DISTINCT [{asset_col}] AS A FROM [{TABLE}] '
        f'WHERE [{location_col}] = ? AND [{asset_col}] IS NOT NULL AND TRIM([{asset_col}]) <> "" '
        f'ORDER BY 1',
        (loc_choice,), db_path=db_path
    )
    assets = [str(x) for x in assets_df["A"].dropna().tolist()]

    asset_choice = st.sidebar.selectbox("Asset", options=(["— Choose asset —"] + assets), index=0)
    if asset_choice == "— Choose asset —":
        st.info("Choose an Asset to see its work order history.")
        st.stop()

    # Query rows for this (Location, Asset)
    where = f'WHERE [{location_col}] = ? AND [{asset_col}] = ?'
    order = f'ORDER BY [{wo_col}] ASC'
    if sort_col and sort_col in cols:
        # numeric-ish ordering for Sort; fallback to text if cast fails
        order = f'ORDER BY [{wo_col}] ASC, CAST([{sort_col}] AS INTEGER) ASC, ROWID'

    sql = f'SELECT * FROM [{TABLE}] {where} {order}'
    df = q(sql, (loc_choice, asset_choice), db_path=db_path)

    # Show Completed Date as date-only (if present)
    if completed_col and completed_col in df.columns:
        d = pd.to_datetime(df[completed_col], errors="coerce", utc=False).dt.date.astype(str)
        # Keep blanks as "" instead of "NaT"
        df[completed_col] = d.where(~d.isna(), "")

    # Title & table
    st.markdown(f"### Work Order History — {loc_choice} — {asset_choice}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads
    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=to_xlsx_bytes(df, sheet="WorkOrders"),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=to_docx_bytes(df, title=f"Work Orders — {loc_choice} — {asset_choice}"),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # Admin-only: config template for quick reference
    if is_admin:
        CONFIG_TEMPLATE = """
        [app_config.cookie]
        name = "wo_portal"
        key = "change_me"
        expiry_days = 7

        [app_config.access]
        admin_usernames = ["brad"]

        [app_config.access.user_locations]
        brad = ["*"]
        dlauer = ["110 - Deckers Creek Limestone", "240 - Buckeye Stone"]
        """
        with st.expander("ℹ️ Config snippet (TOML)"):
            st.code(textwrap.dedent(CONFIG_TEMPLATE).strip(), language="toml")

