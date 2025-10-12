# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders portal
# - Login (streamlit-authenticator)
# - Authorize by Location
# - Required selection flow: Location -> Asset (searchable dropdowns)
# - Table = history for selected Asset at selected Location
# - Preserves DB/view order; displays COMPLETED ON as date-only
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

st.set_page_config(page_title="SPF Work Orders", page_icon="🛠️", layout="wide")

# ---------- Defaults & config ----------
DEFAULT_DB = "maintainx_workorders.db"  # local fallback; Cloud uses secrets→GitHub

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
    tmpdir = Path(tempfile.gettempdir()) / "spf_wo_cache"
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

@st.cache_data(show_spinner=False)
def q(sql: str, params: tuple = (), db_path: str | None = None) -> pd.DataFrame:
    path = db_path or DEFAULT_DB
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=params)

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
                qv = lens.quantile(0.9) if not lens.empty else 10
                qv = 10 if pd.isna(qv) else qv
                width = min(60, max(10, int(qv) + 2))
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
def get_data_last_updated(db_path: str) -> str | None:
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
st.sidebar.caption(f"SPF Work Orders — v{APP_VERSION}")
cfg = load_config()
cfg = to_plain(cfg)

# Auth (pin streamlit-authenticator==0.2.3)
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

    db_path = resolve_db_path(cfg)

    # Sidebar: only the "last updated" info (no DB path)
    updated_label = get_data_last_updated(db_path)
    if updated_label:
        st.sidebar.caption(updated_label)

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()

    # ---- Authorization by Location ----
    username_ci = str(username).casefold()
    admin_users_raw = (cfg.get('access', {}).get('admin_usernames', []) or [])
    admin_users_ci = {str(u).casefold() for u in admin_users_raw}
    is_admin = username_ci in admin_users_ci

    # Case-insensitive lookup of user_locations
    ul_raw = (cfg.get('access', {}).get('user_locations', {}) or {})
    ul_ci_map = {str(k).casefold(): v for k, v in ul_raw.items()}
    allowed_cfg = ul_ci_map.get(username_ci, [])
    if isinstance(allowed_cfg, str):
        allowed_cfg = [allowed_cfg]
    allowed_cfg = [a for a in (allowed_cfg or [])]

    def norm(s: str) -> str:
        return " ".join(str(s).strip().split()).casefold()

    # Pull all distinct Locations present
    loc_df = q("SELECT DISTINCT [Location] FROM [vw_workorders_by_workorder] WHERE [Location] IS NOT NULL AND [Location] <> '' ORDER BY 1", db_path=db_path)
    all_locations = [str(x) for x in loc_df['Location'].dropna().tolist()]

    db_loc_map = {norm(c): c for c in all_locations}
    allowed_norm = {norm(a) for a in allowed_cfg}
    star_granted = any(str(a).strip() == "*" for a in allowed_cfg)

    if is_admin or star_granted:
        allowed_locations = set(all_locations)
    else:
        matches = {db_loc_map[n] for n in allowed_norm if n in db_loc_map}
        allowed_locations = matches if matches else set(allowed_cfg)  # show configured names even if no rows now

    if not allowed_locations:
        st.error("No locations configured for your account. Ask an admin to update your access.")
        with st.expander("Locations present in DB"):
            st.write(sorted(all_locations))
        st.stop()

    # ---- UI: Location then Asset (both required, searchable) ----
    loc_choice = st.sidebar.selectbox(
        "Choose Location",
        options=["— Choose location —"] + sorted(allowed_locations),
        index=0,
    )
    if loc_choice == "— Choose location —":
        st.info("Select a Location on the left.")
        st.stop()

    # Assets present at chosen location (unique)
    asset_df = q(
        "SELECT DISTINCT [ASSET] FROM [vw_workorders_by_workorder] WHERE [Location] = ? AND [ASSET] IS NOT NULL AND [ASSET] <> '' ORDER BY 1",
        (loc_choice,), db_path=db_path
    )
    assets = [str(x) for x in asset_df['ASSET'].dropna().tolist()]
    if not assets:
        st.warning("No assets found for that Location.")
        st.stop()

    asset_choice = st.sidebar.selectbox(
        "Choose Asset",
        options=["— Choose asset —"] + assets,
        index=0,
    )
    if asset_choice == "— Choose asset —":
        st.info("Select an Asset on the left.")
        st.stop()

    # Optional quick search across TITLE / PO / P/N
    search = st.sidebar.text_input('Search Title / PO / P/N (optional)')

    # ---- Query records for the chosen pair (keeps view order) ----
    where = ["[Location] = ?", "[ASSET] = ?"]
    params: list = [loc_choice, asset_choice]

    if search:
        like = f"%{search}%"
        where.append("([TITLE] LIKE ? OR [PO] LIKE ? OR [P/N] LIKE ?)")
        params += [like, like, like]

    sql = f"SELECT * FROM [vw_workorders_by_workorder] WHERE {' AND '.join(where)}"
    df = q(sql, tuple(params), db_path=db_path)

    # ---- Display tweaks ----
    # Show COMPLETED ON as date-only; keep order as-is
    if "COMPLETED ON" in df.columns:
        dt = pd.to_datetime(df["COMPLETED ON"], errors="coerce", utc=False)
        df["COMPLETED ON"] = dt.dt.date.astype(str).where(dt.notna(), "")

    title_txt = f"{loc_choice} — {asset_choice}"
    st.markdown(f"### Work Orders — {title_txt}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Downloads (use the exact same df)
    c1, c2, _ = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            label='⬇️ Excel (.xlsx)',
            data=to_xlsx_bytes(df, sheet="WorkOrders"),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.xlsx".replace(" ", "_"),
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
    with c2:
        st.download_button(
            label='⬇️ Word (.docx)',
            data=to_docx_bytes(df, title=f"Work Orders — {title_txt}"),
            file_name=f"WorkOrders_{loc_choice}_{asset_choice}.docx".replace(" ", "_"),
            mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        )

    # Only admins see the config template
    if is_admin:
        with st.expander('ℹ️ Config template'):
            st.code(textwrap.dedent(CONFIG_TEMPLATE_YAML).strip(), language='yaml')

