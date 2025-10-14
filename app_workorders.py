# app.py  — SPF Work Orders (Parquet + new headers)
from __future__ import annotations

import io, os, re, json
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.16a"

# ---------- third-party (guard rails) ----------
try:
    import streamlit_authenticator as stauth
except Exception:
    st.error("streamlit-authenticator is required. Add it to requirements.txt")
    st.stop()

try:
    from docx import Document
    from docx.shared import Pt
except Exception:
    st.error("python-docx is required. Add it to requirements.txt")
    st.stop()

st.set_page_config(page_title="SPF Work Orders", page_icon="🧰", layout="wide")

# ---------- constants ----------
SHEET_WORKORDERS            = "Workorders"  # legacy history (optional)
SHEET_ASSET_MASTER          = "Asset_Master"
SHEET_WO_MASTER             = "Workorders_Master"  # listing with flags
SHEET_WO_SERVICE_CANDS      = [
    "Workorders_master_Services",  # your current name
    "Workorders_Master_Services",
    "Workorders_Master_service",
    "Workorders_Master_Service",
]
SHEET_SERVICE_CANDIDATES    = ["Service Report", "Service_Report", "ServiceReport"]
SHEET_READING_HISTORY_CANDS = ["Reading_Hidtory", "Reading_History"]  # optional
SHEET_METERS_MASTER         = "Meters_Master"                         # optional

REQUIRED_WO_COLS = ["WORKORDER","TITLE","STATUS","PO","P/N","QUANTITY RECEIVED","Vendors","COMPLETED ON","ASSET","Location"]
OPTIONAL_SORT_COL = "Sort"
ASSET_MASTER_COLS = ["Location","ASSET"]

MASTER_REQUIRED = [
    "ID","Title","Description","Asset","Status","Created on","Planned Start Date",
    "Due date","Started on","Completed on","Assigned to","Teams Assigned to",
    "Completed by","Location","IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"
]

# Canon maps (tolerant)
SERVICE_REPORT_CANON = {
    "WO_ID":{"workorder","wo","work order","work order id","id","wo id"},
    "Asset":{"asset","asset name","name"},
    "Location":{"location","ns location","location2"},
    "Date":{"date","completed on","performed on","service date","closed on"},
    "Last Reading":{"last reading","reading"},
    "Last Service Type":{"last service type"},
    "Date of Last service":{"date of last service"},
    "Hours/Miles at Last Service":{"hours/miles at last service","hours miles at last service"},
    "Next Service Type":{"next service type"},
    "Next Service":{"next service"},
    "Remaining":{"remaining","remaining value","units remaining","miles remaining","hours remaining","reading remaining","remaining units"},
    "Meter Type":{"meter type","type","uom","unit","units"},
    "Today":{"today"},
    "Schedule":{"schedule","interval","frequency","meter interval","planned interval","cycle"},
    "User":{"user","technician","completed by","performed by","assigned to"},
    "Notes":{"notes","description","comment","comments","details"},
    "Status":{"status"},
}

SERVICE_HISTORY_CANON = {
    "WO_ID":{"id","wo","workorder","work order","workorder id"},
    "Title":{"title"},
    "Service":{"service","service type","procedure","task"},
    "Asset":{"asset","asset name","name"},
    "Location2":{"location2","location"},
    "Date":{"completed on","performed on","date","service date"},
    "User":{"completed by","technician","assigned to","performed by","user"},
    "Notes":{"notes","description","comment","comments","details"},
    "Status":{"status"},
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

def _norm_key(x: str) -> str:
    s = re.sub(r"[^0-9a-z]+", " ", str(x).lower())
    return re.sub(r"\s+", " ", s).strip()

def _canonize_headers(df: pd.DataFrame, canon: dict[str, set[str]]) -> pd.DataFrame:
    low_to_orig = {str(c).strip().lower(): str(c) for c in df.columns}
    mapping = {}
    for key, aliases in canon.items():
        key_l = key.lower()
        if key_l in low_to_orig:
            mapping[low_to_orig[key_l]] = key
            continue
        for low, orig in low_to_orig.items():
            low2 = re.sub(r"\s+", " ", low)
            if low in aliases or low2 in aliases:
                mapping[orig] = key
                break
    return df.rename(columns=mapping)

def coerce_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s
    m = s.astype(str).str.strip().str.lower()
    true_vals  = {"true","yes","y","1","t"}
    false_vals = {"false","no","n","0","f","", "nan", "none"}
    out = m.map(lambda x: True if x in true_vals else (False if x in false_vals else False))
    return out.astype(bool)

# ---------- GitHub + “last updated” ----------
def _gh_headers(raw=True, token=None):
    h = {"Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"token {token}"
    return h

def github_latest_commit_iso(repo: str, path: str, branch: str = "main", token: str | None = None) -> str | None:
    try:
        import requests
        url = f"https://api.github.com/repos/{repo}/commits"
        params = {"path": path, "per_page": 1, "sha": branch}
        r = requests.get(url, headers=_gh_headers(raw=False, token=token), params=params, timeout=20)
        r.raise_for_status()
        return r.json()[0]["commit"]["committer"]["date"]  # UTC Z
    except Exception:
        return None

def get_data_last_updated_et(iso_utc: str | None) -> str | None:
    if not iso_utc:
        return None
    try:
        from zoneinfo import ZoneInfo
        dt_utc = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        dt_et  = dt_utc.astimezone(ZoneInfo("America/New_York"))
        return dt_et.strftime("Data last updated: %Y-%m-%d %H:%M ET")
    except Exception:
        return None

def download_xlsx(repo: str, path: str, branch: str, token: str | None) -> bytes:
    import requests
    url1 = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r1 = requests.get(url1, headers=_gh_headers(raw=True, token=token), timeout=30)
    if r1.status_code == 200:
        data = r1.content
    else:
        url2 = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
        r2 = requests.get(url2, headers=_gh_headers(raw=True, token=token), timeout=30)
        if r2.status_code != 200:
            raise RuntimeError(f"GitHub download failed ({r1.status_code}/{r2.status_code}).")
        data = r2.content
    if not data or len(data) < 100:
        raise RuntimeError("Downloaded file is unexpectedly small.")
    head = data[:128].lstrip()
    if head.startswith(b"{") or b"<html" in head.lower():
        raise RuntimeError("Got JSON/HTML instead of raw Excel. Check repo/path.")
    return data

# ---------- Parquet cache ----------
class PQ:
    def __init__(self, base_dir: str, enabled: bool, commit_iso: str | None):
        self.base = Path(base_dir or "spf-data").resolve()
        self.base.mkdir(parents=True, exist_ok=True)
        self.enabled = bool(enabled)
        self.commit_iso = commit_iso or ""
        self.meta_file = self.base / "_meta.json"
        self.meta = {}
        if self.meta_file.exists():
            try:
                self.meta = json.loads(self.meta_file.read_text(encoding="utf-8"))
            except Exception:
                self.meta = {}

    def _fname(self, sheet: str) -> Path:
        safe = re.sub(r"[^0-9A-Za-z_.-]+", "_", sheet)
        return self.base / f"{safe}.parquet"

    def read(self, sheet: str) -> pd.DataFrame | None:
        if not self.enabled:
            return None
        f = self._fname(sheet)
        if not f.exists():
            return None
        # stale if commit changed
        wanted = self.commit_iso
        have = (self.meta.get("sheets", {}) or {}).get(sheet, "")
        if wanted and have and wanted != have:
            return None
        try:
            return pd.read_parquet(f)
        except Exception:
            return None

    def write(self, sheet: str, df: pd.DataFrame):
        if not self.enabled:
            return
        f = self._fname(sheet)
        try:
            df.to_parquet(f, index=False)
            m = self.meta.get("sheets", {}) or {}
            m[sheet] = self.commit_iso
            self.meta["sheets"] = m
            self.meta_file.write_text(json.dumps(self.meta, indent=2), encoding="utf-8")
        except Exception:
            pass

# ---------- data loaders (Parquet-first) ----------
def load_sheet_df(xlsx_bytes: bytes, sheet: str, usecols: list[str] | None = None) -> pd.DataFrame:
    return pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
        usecols=usecols
    )

def load_asset_master(xlsx_bytes: bytes, pq: PQ) -> pd.DataFrame:
    cached = pq.read(SHEET_ASSET_MASTER)
    if cached is not None:
        return cached
    df = load_sheet_df(xlsx_bytes, SHEET_ASSET_MASTER, usecols=ASSET_MASTER_COLS)
    df.columns = [str(c).strip() for c in df.columns]
    for c in ASSET_MASTER_COLS:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df = df[(df["Location"] != "") & (df["ASSET"] != "")]
    pq.write(SHEET_ASSET_MASTER, df)
    return df

def load_workorders_master(xlsx_bytes: bytes, pq: PQ) -> pd.DataFrame:
    cached = pq.read(SHEET_WO_MASTER)
    if cached is not None:
        return cached
    df = load_sheet_df(xlsx_bytes, SHEET_WO_MASTER)
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
    pq.write(SHEET_WO_MASTER, df)
    return df

def load_workorders_history(xlsx_bytes: bytes, pq: PQ) -> pd.DataFrame | None:
    cached = pq.read(SHEET_WORKORDERS)
    if cached is not None:
        return cached
    try:
        df = load_sheet_df(xlsx_bytes, SHEET_WORKORDERS)
    except Exception:
        return None
    df.columns = [str(c).strip() for c in df.columns]
    if all(c in df.columns for c in REQUIRED_WO_COLS):
        df = df[[*REQUIRED_WO_COLS, *( [OPTIONAL_SORT_COL] if OPTIONAL_SORT_COL in df.columns else [] )]].copy()
        df["COMPLETED ON"] = df["COMPLETED ON"].map(_norm_date_any)
        for c in df.columns:
            df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
    pq.write(SHEET_WORKORDERS, df)
    return df

def load_service_report(xlsx_bytes: bytes, pq: PQ):
    for nm in SHEET_SERVICE_CANDIDATES:
        cached = pq.read(nm)
        if cached is not None:
            canon = _canonize_headers(cached.copy(), SERVICE_REPORT_CANON)
            return cached, canon, nm
        try:
            raw = load_sheet_df(xlsx_bytes, nm)
            raw.columns = [str(c).strip() for c in raw.columns]
            canon = _canonize_headers(raw.copy(), SERVICE_REPORT_CANON)
            # normalize key fields
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
                canon["__PctRemain_num"] = pr.where((pr.isna()) | (pr <= 1.0), pr/100.0)
            if "Meter Type" in canon.columns:
                canon["__MeterType_norm"] = canon["Meter Type"].astype(str).str.strip().str.lower()
            else:
                canon["__MeterType_norm"] = ""
            if "Due Date" in canon.columns:
                canon["__Due_dt"] = pd.to_datetime(canon["Due Date"], errors="coerce")
            else:
                canon["__Due_dt"] = pd.NaT
            for c in [x for x in ["WO_ID","Asset","Location","User","Notes","Status"] if x in canon.columns]:
                canon[c] = canon[c].astype(str).str.strip()
            pq.write(nm, raw)
            # We do not store canon separately; recompute quickly when reading.
            return raw, canon, nm
        except Exception:
            continue
    return None, None, None

def load_service_history(xlsx_bytes: bytes, pq: PQ):
    last_err = None
    for nm in SHEET_WO_SERVICE_CANDS:
        cached = pq.read(nm)
        if cached is not None:
            df = _canonize_headers(cached.copy(), SERVICE_HISTORY_CANON)
            if "Date" in df.columns:
                df["Date"] = df["Date"].map(_norm_date_any)
            for c in [x for x in ["WO_ID","Title","Service","Asset","Location2","User","Notes","Status"] if x in df.columns]:
                df[c] = df[c].astype(str).str.strip()
            keep = [c for c in ["Date","WO_ID","Title","Service","Asset","User","Location2","Notes","Status"] if c in df.columns]
            return (df[keep].copy() if keep else df), nm
        try:
            raw = load_sheet_df(xlsx_bytes, nm)
            raw.columns = [str(c).strip() for c in raw.columns]
            df = _canonize_headers(raw, SERVICE_HISTORY_CANON)
            if "Date" in df.columns:
                df["Date"] = df["Date"].map(_norm_date_any)
            for c in [x for x in ["WO_ID","Title","Service","Asset","Location2","User","Notes","Status"] if x in df.columns]:
                df[c] = df[c].astype(str).str.strip()
            keep = [c for c in ["Date","WO_ID","Title","Service","Asset","User","Location2","Notes","Status"] if c in df.columns]
            out = df[keep].copy() if keep else df
            pq.write(nm, raw)
            return out, nm
        except Exception as e:
            last_err = e
            continue
    return None, f"{last_err}" if last_err else None

# ---------- Word/Excel exports ----------
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

# ---------- App ----------
st.sidebar.caption(f"SPF Work Orders — v{APP_VERSION}")

cfg = load_config()
cfg = to_plain(cfg)
settings = (cfg.get("settings") or {})
use_parquet = bool(settings.get("use_parquet", True))
db_dir = settings.get("db_dir", "spf-data")

gh = st.secrets.get("github") if hasattr(st, "secrets") else cfg.get("github", {})
repo   = (gh or {}).get("repo")
path   = (gh or {}).get("path")
branch = (gh or {}).get("branch", "main")
token  = (gh or {}).get("token")

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

    # get latest commit (for ET label + cache freshness)
    latest_iso = github_latest_commit_iso(repo, path, branch, token) if (repo and path) else None
    label = get_data_last_updated_et(latest_iso)
    if label:
        st.sidebar.caption(label)

    if st.sidebar.button("🔄 Refresh data"):
        # blow away Streamlit cache AND Parquet meta to force reload
        st.cache_data.clear()
        try:
            meta = Path(db_dir) / "_meta.json"
            if meta.exists():
                meta.unlink()
        except Exception:
            pass
        st.rerun()

    # download workbook
    if repo and path:
        try:
            xlsx_bytes = download_xlsx(repo, path, branch, token)
        except Exception as e:
            st.error(f"Could not load Excel: {e}")
            st.stop()
    else:
        st.error("Missing [github] repo/path config.")
        st.stop()

    pq = PQ(db_dir, use_parquet, latest_iso)

    # Access control: Locations
    try:
        df_am = load_asset_master(xlsx_bytes, pq)
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

    page = st.sidebar.radio(
        "Page",
        ["📋 Work Orders", "🧾 Service Report", "📚 Service History", "🔎 Asset History"],
        index=0,
    )

    # ========= Work Orders =========
    if page == "📋 Work Orders":
        st.markdown("### Work Orders — Filtered Views (flags from workbook)")
        try:
            df_master = load_workorders_master(xlsx_bytes, pq)
        except Exception as e:
            st.error(f"Failed to read '{SHEET_WO_MASTER}': {e}")
            st.stop()

        # restrict to allowed locations
        df_master = df_master[df_master["Location"].isin(allowed_locations)].copy()
        total_in_scope = len(df_master)

        # optional Users list
        derived_users = sorted([u for u in df_master.get("Assigned to", pd.Series([], dtype=str))
                               .dropna().astype(str).str.strip().unique().tolist() if u])

        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        with c1:
            loc_values = sorted(df_master["Location"].dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values, index=0)
        df_scope = df_master if chosen_loc == loc_all_label else df_master[df_master["Location"] == chosen_loc].copy()

        with c2:
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
            view = st.radio("View", ["All","Open","Overdue","Scheduled (Planning)","Completed","Old"], horizontal=True, index=1)

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
        raw_sr, canon_sr, source_sheet = load_service_report(xlsx_bytes, pq)
        if raw_sr is None:
            st.warning("No 'Service Report' sheet found.")
            st.stop()

        # Location filter (report itself stays otherwise unfiltered)
        loc_col = None
        for c in raw_sr.columns:
            if c.strip().lower() in {"location","ns location","location2"}:
                loc_col = c; break
        if loc_col:
            loc_values = sorted([v for v in raw_sr[loc_col].astype(str).unique().tolist() if _norm_key(v) in allowed_norms])
        else:
            loc_values = sorted(allowed_locations)

        loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
        chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label], index=0)

        if loc_col and chosen_loc != loc_all_label:
            raw_show = raw_sr[_norm_key(raw_sr[loc_col]) == _norm_key(chosen_loc)].copy()
            canon_in_scope = canon_sr[_norm_key(canon_sr["Location"]) == _norm_key(chosen_loc)] if "Location" in canon_sr.columns else canon_sr.copy()
        else:
            raw_show = raw_sr.copy()
            canon_in_scope = canon_sr.copy()

        t_report, t_due, t_over = st.tabs(["Report", "Coming Due", "Overdue"])

        with t_report:
            st.caption(f"Source: {source_sheet}  •  Rows: {len(raw_show)}  •  No filters other than Location.")
            st.dataframe(raw_show, use_container_width=True, hide_index=True)
            c1, c2, _ = st.columns([1,1,6])
            with c1:
                st.download_button("⬇️ Excel (.xlsx)", data=to_xlsx_bytes(raw_show, sheet="Service_Report"),
                                   file_name="Service_Report.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                st.download_button("⬇️ Word (.docx)", data=to_docx_bytes(raw_show, title="Service Report — As Is"),
                                   file_name="Service_Report.docx",
                                   mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        # Coming-due / Overdue
        def _due_frames(df_can: pd.DataFrame):
            if df_can is None or df_can.empty:
                return pd.DataFrame(), pd.DataFrame()
            df = df_can.copy()
            def row_threshold(r) -> float:
                mt = str(r.get("__MeterType_norm","")).lower()
                return 0.05 if ("mile" in mt) else 0.10
            conds = []
            condA = (
                df.get("__Schedule_num").notna() &
                (pd.to_numeric(df.get("__Schedule_num"), errors="coerce") > 0) &
                df.get("__Remaining_num").notna() &
                (pd.to_numeric(df.get("__Remaining_num"), errors="coerce") >= 0)
            )
            if condA.any():
                thrA = df.apply(row_threshold, axis=1)
                condA2 = pd.to_numeric(df["__Remaining_num"], errors="coerce") <= (pd.to_numeric(df["__Schedule_num"], errors="coerce") * thrA)
                conds.append(condA & condA2)
            if "__PctRemain_num" in df.columns:
                condB = df["__PctRemain_num"].notna()
                if condB.any():
                    thrB = df.apply(row_threshold, axis=1)
                    condB2 = pd.to_numeric(df["__PctRemain_num"], errors="coerce") <= thrB
                    conds.append(condB & condB2)
            coming_due_mask = pd.Series(False, index=df.index)
            for c in conds:
                coming_due_mask |= c
            coming_due = df[coming_due_mask].copy()
            today = pd.Timestamp.today().normalize()
            overdue_mask = pd.Series(False, index=df.index)
            if "__Remaining_num" in df.columns:
                overdue_mask |= (pd.to_numeric(df["__Remaining_num"], errors="coerce") < 0)
            if "__Due_dt" in df.columns:
                overdue_mask |= (pd.to_datetime(df["__Due_dt"], errors="coerce") < today)
            overdue = df[overdue_mask].copy()
            return coming_due, overdue

        coming_due_df, overdue_df = _due_frames(canon_in_scope)

        def present_due(df: pd.DataFrame):
            base = ["WO_ID","Asset","Location","Date","User","Status",
                    "Schedule","Remaining","Percent Remaining","Meter Type","Due Date","Notes"]
            cols = [c for c in base if c in df.columns]
            return df[cols] if cols else df

        with t_due:
            st.caption("Coming Due = Remaining ≤ 10% of Schedule (or ≤ 5% if Meter Type contains 'miles').")
            if coming_due_df.empty:
                st.info("No items are coming due based on Schedule/Remaining/% Remaining.")
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
                st.info("No overdue items found.")
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
        st.stop()

    # ========= Service History (Location2!) =========
    if page == "📚 Service History":
        st.markdown("### Service History")
        df_hist, used_sheet_or_err = load_service_history(xlsx_bytes, pq)
        if df_hist is None or df_hist.empty:
            st.warning(f"No Service History data. Tried {SHEET_WO_SERVICE_CANDS}. Last error: {used_sheet_or_err}")
            st.stop()

        # Restrict by allowed locations (uses Location2)
        if "Location2" in df_hist.columns:
            df_hist["__LocNorm"] = df_hist["Location2"].map(_norm_key)
            df_hist = df_hist[df_hist["__LocNorm"].isin(allowed_norms)].copy()

        # Filters
        c1, c2 = st.columns([2, 3])
        with c1:
            if "Location2" in df_hist.columns:
                loc_values = sorted(df_hist["Location2"].dropna().unique().tolist())
            else:
                loc_values = []
            loc_all_label = f"« All my locations ({len(loc_values)}) »" if loc_values else "« All my locations »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values if loc_values else [loc_all_label], index=0)

        if chosen_loc != loc_all_label and "Location2" in df_hist.columns:
            scope = df_hist[_norm_key(df_hist["Location2"]) == _norm_key(chosen_loc)].copy()
        else:
            scope = df_hist.copy()

        with c2:
            assets = sorted([a for a in scope.get("Asset", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if a])
            sel_asset = st.selectbox("Asset", options=assets, index=0 if assets else None)

        if not assets:
            st.info("No assets available in this Location.")
            st.stop()

        if "Asset" in scope.columns:
            scope = scope[scope["Asset"] == sel_asset]

        if "Date" in scope.columns:
            scope = scope.copy()
            scope["__Date_dt"] = pd.to_datetime(scope["Date"], errors="coerce")
            scope = scope.sort_values(by="__Date_dt", ascending=False, na_position="last").drop(columns="__Date_dt")

        show_cols = [c for c in ["Date","WO_ID","Title","Service","Asset","User","Location2","Notes","Status"] if c in scope.columns]
        st.caption(f"Sheet used: {used_sheet_or_err} • Rows: {len(scope)}")
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
        st.stop()

    # ========= Asset History (legacy) =========
    if page == "🔎 Asset History":
        st.markdown("### Asset History")
        df_all = load_workorders_history(xlsx_bytes, pq)
        if df_all is None or df_all.empty:
            st.info("Legacy 'Workorders' sheet not found in the workbook.")
            st.stop()

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

        df = df_all[(df_all["Location"] == chosen_loc) & (df_all["ASSET"] == chosen_asset)].copy()
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
        df_out = df.drop(columns=["__row","__sort_key", OPTIONAL_SORT_COL], errors="ignore")

        st.dataframe(df_out, use_container_width=True, hide_index=True)

        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            st.download_button(
                label="⬇️ Excel (.xlsx)",
                data=to_xlsx_bytes(df_out, sheet="Workorders"),
                file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.xlsx".replace(" ","_"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with c2:
            st.download_button(
                label="⬇️ Word (.docx)",
                data=to_docx_bytes(df_out, title=f"Work Orders — {chosen_loc} — {chosen_asset}"),
                file_name=f"WorkOrders_{chosen_loc}_{chosen_asset}.docx".replace(" ","_"),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        st.stop()


