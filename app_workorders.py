# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Location -> Asset selectors (searchable)
# - Shows asset history from Workorders.xlsx
# - Keeps WO→PO→TRANS order via optional "Sort" column (1,2,3)
# - Blanks WORKORDER only on Sort in {2,3} and hides "Sort" column
# - "COMPLETED ON" normalized to YYYY-MM-DD
# - Robust GitHub download + "Data last updated" from latest commit
# --------------------------------------------------------------

from __future__ import annotations
import io, textwrap
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
from zipfile import BadZipFile

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.12"

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
SHEET_WORKORDERS   = "Workorders"
SHEET_ASSET_MASTER = "Asset_Master"

REQUIRED_WO_COLS = [
    "WORKORDER", "TITLE", "STATUS", "PO", "P/N", "QUANTITY RECEIVED",
    "Vendors", "COMPLETED ON", "ASSET", "Location",
]
OPTIONAL_SORT_COL = "Sort"       # if present, 1=WO, 2=PO, 3=TRANS

ASSET_MASTER_COLS = ["Location", "ASSET"]


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


@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED_WO_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")

    # Keep exact required order + optional Sort at the end if present
    cols = REQUIRED_WO_COLS[:]
    if OPTIONAL_SORT_COL in df.columns:
        cols = cols + [OPTIONAL_SORT_COL]
    df = df[cols].copy()

    # Normalize date column to YYYY-MM-DD
    def norm_date(s: str) -> str:
        s = (s or "").strip()
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

    df["COMPLETED ON"] = df["COMPLETED ON"].map(norm_date)

    for c in df.columns:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df


@st.cache_data(show_spinner=False)
def load_asset_master_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in ASSET_MASTER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")
    for c in ASSET_MASTER_COLS:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[(df["Location"] != "") & (df["ASSET"] != "")]
    return df[ASSET_MASTER_COLS].copy()


def get_data_last_updated() -> str | None:
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh or not gh.get("repo") or not gh.get("path"):
        return None
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
        return None


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

    try:
        xlsx_bytes = get_xlsx_bytes(cfg)
    except Exception as e:
        st.error(f"Could not load Excel: {e}")
        st.stop()

    try:
        df_all = load_workorders_df(xlsx_bytes, SHEET_WORKORDERS)
        df_am  = load_asset_master_df(xlsx_bytes, SHEET_ASSET_MASTER)
    except BadZipFile:
        st.error("The downloaded file isn’t a valid .xlsx. Check your [github] repo/path/branch/token.")
        st.stop()
    except Exception as e:
        st.error(f"Failed to read Excel: {e}")
        st.stop()

    # ---- Access control by Location ----
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

    # --- UI: Location / Asset ---
    loc_placeholder = "— Choose Location —"
    loc_options = [loc_placeholder] + sorted(allowed_locations)
    chosen_loc = st.sidebar.selectbox("Location", options=loc_options, index=0)
    if chosen_loc == loc_placeholder:
        st.info("Select a Location to load Assets.")
        st.stop()

    assets_for_loc = sorted(df_am.loc[df_am["Location"] == chosen_loc, "ASSET"].dropna().unique().tolist())
    asset_placeholder = "— Choose Asset —"
    asset_options = [asset_placeholder] + assets_for_loc
    chosen_asset = st.sidebar.selectbox("Asset", options=asset_options, index=0)
    if chosen_asset == asset_placeholder:
        st.info("Select an Asset to view its history.")
        st.stop()

    # --- Filter to selection ---
    df = df_all[(df_all["Location"] == chosen_loc) & (df_all["ASSET"] == chosen_asset)].copy()

    # --- Order: WORKORDER ASC, then Sort ASC (1=WO,2=PO,3=TRANS), then stable by original row ---
    df["__row"] = range(len(df))
    if OPTIONAL_SORT_COL in df.columns:
        df["__sort_key"] = pd.to_numeric(df[OPTIONAL_SORT_COL], errors="coerce").fillna(1).astype(int)
    else:
        df["__sort_key"] = 1

    df.sort_values(by=["WORKORDER", "__sort_key", "__row"], ascending=[True, True, True], inplace=True)

    # --- Blank WORKORDER only for Sort in {2,3} ---
    df.loc[df["__sort_key"].isin([2, 3]), "WORKORDER"] = ""

    # --- Build display/download frame without helpers and without Sort column ---
    drop_cols = ["__row", "__sort_key"]
    if OPTIONAL_SORT_COL in df.columns:
        drop_cols.append(OPTIONAL_SORT_COL)
    df_out = df.drop(columns=drop_cols, errors="ignore")

    # --- Show ---
    st.markdown(f"### Work Orders — {chosen_loc} — {chosen_asset}")
    if df_out.empty:
        st.info("No history found for this Asset.")
    st.dataframe(df_out, use_container_width=True, hide_index=True)

    # --- Downloads ---
    c1, c2, _ = st.columns([1, 1, 3])
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

    # Admin-only: minimal config template
    if is_admin:
        CONFIG_TEMPLATE = """
        credentials:
          usernames:
            demo:
              name: Demo User
              email: demo@example.com
              password: "$2b$12$y2J3Y0rRrJ3fA76h2o//mO6F1T0m3b1vS7QhQ4bW5iX9b5b5b5b5e"

        cookie:
          name: spf_workorders_portal
          key: super_secret_key
          expiry_days: 7

        access:
          admin_usernames: [demo]
          user_locations:
            demo: ['*']

        settings:
          # Optional local dev path to the workbook (bypasses GitHub)
          # xlsx_path: "C:/Users/Brad/Desktop/App Master/Workorders.xlsx"
        """
        with st.expander("ℹ️ Config template"):
            st.code(textwrap.dedent(CONFIG_TEMPLATE).strip(), language="yaml")



