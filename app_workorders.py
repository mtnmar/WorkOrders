# app_workorders.py
# --------------------------------------------------------------
# Work Orders portal
# - Login (streamlit-authenticator)
# - Authorize & filter by Location
# - Pick Location (required) → pick Asset (type-ahead)
# - Shows full history (rows) for the chosen Asset (and Location scope)
# - Preserves table column order from the SQLite DB
# - Downloads: Excel (.xlsx) and Word (.docx)
#
# DB source: maintainx_workorders.db (via Streamlit secrets [github] or local)
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
import os, io, sqlite3, textwrap
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.12"

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

# ---------- Defaults & config ----------
DEFAULT_DB = "maintainx_workorders.db"   # local fallback

CONFIG_TEMPLATE_YAML = """
credentials:
  usernames:
    demo:
      name: Demo User
      email: demo@example.com
      password: "$2b$12$y2J3Y0rRrJ3fA76h2o//mO6F1T0m3b1vS7QhQ4bW5iX9b5b5b5b5e"

cookie:
  name: wo_portal_v1
  key: super_secret_key_wo
  expiry_days: 7

access:
  admin_usernames: [demo]
  user_locations:
    demo: ['*']    # '*' = all locations

settings:
  db_path: ""
"""

HERE = Path(__file__).resolve().parent

# ---------- helpers ----------
def to_plain(obj):
    """Recursively convert Secrets to plain Python structures."""
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj

def resolve_db_path(cfg: dict) -> str:
    # 1) YAML/secrets-configured path
    yaml_db = (cfg or {}).get('settings', {}).get('db_path')
    if yaml_db:
        return yaml_db
    # 2) SPF_DB_PATH env
    env_db = os.environ.get('SPF_DB_PATH')
    if env_db:
        return env_db
    # 3) Secrets → GitHub download (supports private repo)
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh:
        try:
            return download_db_from_github(
                repo=gh.get('repo'),
                path=gh.get('path'),
                branch=gh.get('branch', 'main'),
                token=gh.get('token'),
            )
        except Exception as e:
            st.error(f"Failed to download DB from GitHub: {e}")
    # 4) Fallback local
    return DEFAULT_DB

def download_db_from_github(*, repo: str, path: str, branch: str = 'main', token: str | None = None) -> str:
    if not repo or not path:
        raise ValueError("Missing repo/path for GitHub download.")
    import requests, tempfile
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub API returned {r.status_code}: {r.text[:200]}")
    tmpdir = Path(tempfile.gettempdir()) / "workorders_cache"
    tmpdir.mkdir(parents=True, exist_ok=True)
    out = tmpdir / "maintainx_workorders.db"
    out.write_bytes(r.content)
    return str(out)

def load_config() -> dict:
    if "app_config" in st.secrets:           # TOML secrets (recommended)
        return to_plain(st.secrets["app_config"])
    if "app_config_yaml" in st.secrets:       # legacy YAML in secrets
        try:
            return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}")
            return {}
    cfg_file = HERE / "app_config.yaml"       # local file for dev
    if cfg_file.exists():
        try:
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    return yaml.safe_load(CONFIG_TEMPLATE_YAML)

def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

def table_columns_in_order(db_path: str, table: str) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]  # PRAGMA preserves on-disk order

# ---- SAFE Excel export (works even when df is empty) ----
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

# ---- "Data last updated" helper (GitHub commit time or local mtime) ----
def get_data_last_updated(cfg: dict, db_path: str) -> str | None:
    gh = st.secrets.get('github') if hasattr(st, 'secrets') else None
    if gh and gh.get('repo') and gh.get('path'):
        try:
            import requests
            url = f"https://api.github.com/repos/{gh['repo']}/commits"
            params = {"path": gh["path"], "per_page": 1, "sha": gh.get("branch", "main")}
            headers = {"Accept": "application/vnd.github+json"}
            if gh.get("token"):
                headers["Authorization"] = f"token {gh['token']}"
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            iso = r.json()[0]["commit"]["committer"]["date"]  # e.g., '2025-10-11T21:07:33Z'
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
cfg = to_plain(cfg)  # ensure plain dicts

# Auth (pin streamlit-authenticator==0.2.3)
cookie_cfg = cfg.get('cookie', {})
auth = stauth.Authenticate(
    cfg.get('credentials', {}),
    cookie_cfg.get('name', 'wo_portal_v1'),
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

    db_path = resolve_db_path(cfg)

    # Sidebar: show only "last updated"
    updated_label = get_data_last_updated(cfg, db_path)
    if updated_label:
        st.sidebar.caption(updated_label)

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()

    # --- Authorization by Location (case-insensitive usernames, lenient match) ---
    all_locations_df = q(
        "SELECT DISTINCT [Location] FROM [workorders] WHERE [Location] IS NOT NULL ORDER BY 1",
        db_path=db_path
    )
    all_locations = [str(x) for x in all_locations_df['Location'].dropna().tolist()] or []

    username_ci = str(username).casefold()
    admin_users_raw = (cfg.get('access', {}).get('admin_usernames', []) or [])
    admin_users_ci = {str(u).casefold() for u in admin_users_raw}
    is_admin = username_ci in admin_users_ci

    uc_raw = (cfg.get('access', {}).get('user_locations', {}) or {})
    uc_ci_map = {str(k).casefold(): v for k, v in uc_raw.items()}
    allowed_cfg = uc_ci_map.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    def norm(s: str) -> str:
        return " ".join(str(s).strip().split()).casefold()

    db_loc_map = {norm(c): c for c in all_locations}   # normalized -> DB original
    allowed_norm = {norm(a) for a in allowed_cfg}
    star_granted = any(str(a).strip() == "*" for a in allowed_cfg)

    if is_admin or star_granted:
        allowed_set = set(all_locations)
    else:
        matches = {db_loc_map[n] for n in allowed_norm if n in db_loc_map}
        allowed_set = matches or set(allowed_cfg)  # show configured names even if currently not present

    if not allowed_set:
        st.error("No locations configured for your account. Ask an admin to update your access.")
        with st.expander("Locations present in DB"):
            st.write(sorted(all_locations))
        st.stop()

    # ---- Step 1: choose Location (required, searchable)
    loc_options = sorted(allowed_set)
    ADMIN_ALL = "« All locations (admin) »"

    choose_loc_opts = ["— Choose location —"]
    if is_admin and len(all_locations) > 1:
        choose_loc_opts += [ADMIN_ALL]
    choose_loc_opts += loc_options

    chosen_location = st.sidebar.selectbox("Choose Location", options=choose_loc_opts, index=0)
    if chosen_location == "— Choose location —":
        st.info("Select a Location on the left to load assets.")
        st.stop()

    # Determine which locations are in-scope for querying assets
    if is_admin and chosen_location == ADMIN_ALL:
        scoped_locations = sorted(all_locations)
        title_scope = "All locations (admin)"
    else:
        scoped_locations = [chosen_location]
        title_scope = chosen_location

    # ---- Step 2: choose Asset within the scoped locations (required, searchable)
    ph = ",".join(["?"] * len(scoped_locations))
    assets_df = q(
        f"SELECT DISTINCT [ASSET] FROM [workorders] "
        f"WHERE [ASSET] IS NOT NULL AND [ASSET] <> '' AND [Location] IN ({ph}) "
        f"ORDER BY 1",
        tuple(scoped_locations),
        db_path=db_path
    )
    assets = [str(x) for x in assets_df["ASSET"].dropna().tolist()]

    asset_choice = st.sidebar.selectbox("Choose Asset", options=["— Choose asset —"] + assets, index=0)
    if asset_choice == "— Choose asset —":
        st.info("Select an Asset on the left to view its work order history.")
        st.stop()

    # ---- Query full history for the chosen asset (scoped by locations)
    where = [f"[Location] IN ({ph})", "[ASSET] = ?"]
    params: list = list(scoped_locations) + [asset_choice]

    # Order most-recent first if SQLite can parse the date
    sql = (
        "SELECT * FROM [workorders] "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY date([COMPLETED ON]) DESC, [WORKORDER] DESC"
    )
    df = q(sql, tuple(params), db_path=db_path)

    # Preserve on-disk table column order
    cols_in_order = table_columns_in_order(db_path, "workorders")
    df = df[[c for c in cols_in_order if c in df.columns]]

    # Title & grid
    st.markdown(f"### Work Order history — {asset_choice}  ·  {title_scope}")
    if df.empty:
        st.warning("No rows found for that selection.")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads (use the exact same df)
    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=to_xlsx_bytes(df, sheet="Workorders"),
            file_name=f"Workorders_{asset_choice}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=to_docx_bytes(df, title=f"Work Orders — {asset_choice} — {title_scope}"),
            file_name=f"Workorders_{asset_choice}.docx",
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # Admin-only: show config template
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')
