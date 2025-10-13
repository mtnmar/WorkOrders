# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Select Location (searchable), then Asset (searchable)
# - Show that Asset's history (from Workorders sheet)
# - "Completed On" normalized to YYYY-MM-DD (date only)
# - Robust GitHub bytes download (no BadZipFile)
# - Sidebar shows "Data last updated" (GitHub commit time or file mtime)
#
# Secrets (Streamlit Cloud -> Settings -> Secrets):
#   [github]
#   repo = "YOURUSER/spf-data"
#   path = "Workorders.xlsx"            # or "folder/Workorders.xlsx"
#   branch = "main"
#   token = "ghp_..."                   # token with repo read access
#
#   [app_config]
#   # streamlit-authenticator credentials (bcrypt hashes)
#   credentials.usernames.brad.name = "Brad"
#   credentials.usernames.brad.email = "brad@example.com"
#   credentials.usernames.brad.password = "$2b$12$....."   # bcrypt
#
#   cookie.name = "spf_workorders_portal"
#   cookie.key  = "super_secret_key"
#   cookie.expiry_days = 7
#
#   access.admin_usernames = ["brad"]
#   # Map usernames (case-insensitive) -> locations (exact strings)
#   # Example:
#   access.user_locations.brad = ["*"]
#   access.user_locations.dlauer = ["110 - Deckers Creek Limestone", "240 - Buckeye Stone"]
#
# Optional local dev override (bypass GitHub):
#   [app_config.settings]
#   xlsx_path = "C:/Users/Brad/Desktop/App Master/Workorders.xlsx"
#
# Requirements: see requirements.txt in this repo.
# --------------------------------------------------------------

from __future__ import annotations
import io, os, textwrap
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
SHEET_WORKORDERS = "Workorders"
SHEET_ASSET_MASTER = "Asset_Master"
SORT_COL = "Sort"  # optional helper column in your Excel

REQUIRED_WO_COLS_BASE = [
    "WORKORDER", "TITLE", "STATUS", "PO", "P/N", "QUANTITY RECEIVED",
    "Vendors", "COMPLETED ON", "ASSET", "Location",
]

ASSET_MASTER_COLS = ["Location", "ASSET"]  # Asset_Master sheet expected headers


# ---------- helpers ----------
def to_plain(obj):
    """Convert Secrets/TOML containers to plain Python dict/list."""
    if isinstance(obj, Mapping):
        return {k: to_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_plain(x) for x in obj]
    return obj


def load_config() -> dict:
    # Prefer TOML secrets block [app_config]
    if "app_config" in st.secrets:
        return to_plain(st.secrets["app_config"])
    # Fallback: allow YAML string secret
    if "app_config_yaml" in st.secrets:
        try:
            return yaml.safe_load(st.secrets["app_config_yaml"]) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config_yaml secret: {e}")
            return {}
    # Local fallback file (dev)
    here = Path(__file__).resolve().parent
    cfg_file = here / "app_config.yaml"
    if cfg_file.exists():
        try:
            return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        except Exception as e:
            st.error(f"Invalid YAML in app_config.yaml: {e}")
            return {}
    # Minimal default
    return {}


# --- robust GitHub file downloader (supports private repos) ---
def download_bytes_from_github_file(*, repo: str, path: str, branch: str = "main", token: str | None = None) -> bytes:
    """
    Fetch raw file bytes from a GitHub repo. Tries the Contents API first,
    then raw.githubusercontent.com as a fallback. Validates that we didn't
    get HTML/JSON by mistake.
    """
    import requests

    def _headers(raw: bool = True):
        h = {"Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"}
        if token:
            h["Authorization"] = f"token {token}"
        return h

    # 1) GitHub Contents API (works for private repos)
    url1 = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r1 = requests.get(url1, headers=_headers(raw=True), timeout=30)
    if r1.status_code == 200:
        data = r1.content
    else:
        # 2) Raw URL fallback
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

    # Sanity checks: ensure it's not JSON/HTML/LFS pointer
    if not data or len(data) < 100:
        raise RuntimeError("Downloaded file is unexpectedly small. Check repo/path/branch.")
    head = data[:128].lstrip()
    if head.startswith(b"{") or b"<html" in head.lower():
        raise RuntimeError("Got JSON/HTML instead of raw Excel. Check repo/path/branch/token in secrets.")
    return data


def get_xlsx_bytes(cfg: dict) -> bytes:
    """Return Excel bytes from either local path override or GitHub secrets."""
    # Local override for dev:
    xlsx_path = (cfg.get("settings", {}) or {}).get("xlsx_path")
    if xlsx_path:
        p = Path(xlsx_path)
        if not p.exists():
            raise FileNotFoundError(f"Local Excel not found: {xlsx_path}")
        return p.read_bytes()

    # GitHub (Cloud)
    gh = st.secrets.get("github") if hasattr(st, "secrets") else None
    if not gh:
        raise RuntimeError("No [github] secrets found. Configure repo/path/branch/token in Secrets.")
    return download_bytes_from_github_file(
        repo=gh.get("repo"),
        path=gh.get("path"),
        branch=gh.get("branch", "main"),
        token=gh.get("token"),
    )


@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    """Load Workorders sheet as strings, keep blanks, enforce required columns & normalize dates.
       Keeps the original Excel column order (so Sort is retained if present)."""
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,  # keep "" for blank cells
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in REQUIRED_WO_COLS_BASE if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}\nFound: {list(df.columns)}")

    # Normalize "COMPLETED ON" to YYYY-MM-DD (date only), keep "" if blank
    def norm_date(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        # try pandas parse as last resort
        try:
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt):
                return s  # leave as-is if unparsable
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return s

    if "COMPLETED ON" in df.columns:
        df["COMPLETED ON"] = df["COMPLETED ON"].map(norm_date)

    # Strip whitespace in all cells
    for c in df.columns:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)

    return df


@st.cache_data(show_spinner=False)
def load_asset_master_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    """Load Asset_Master sheet -> columns: Location, ASSET. Drop blanks."""
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
    # Drop rows where either col is empty
    df = df[(df["Location"] != "") & (df["ASSET"] != "")]
    return df[ASSET_MASTER_COLS].copy()


def get_data_last_updated() -> str | None:
    """Commit time for the file (if using GitHub), else None."""
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
        iso = r.json()[0]["commit"]["comm


