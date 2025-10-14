# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Sidebar: choose page (History vs Work Orders)
# - Top-of-page filters for Work Orders (Location, User, Team, View)
# - Workorders_Master already includes: Location, IsOpen, IsOverdue, IsScheduled, IsCompleted, IsOld
# - Never reveal assignments outside allowed locations; if a selected user has
#   no WOs at the chosen location, show "User not found at this location."
# - Dates normalized to YYYY-MM-DD where needed
# - Robust GitHub download + "Data last updated" from latest commit
# --------------------------------------------------------------

from __future__ import annotations
import io, re
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
from zipfile import BadZipFile

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.14"

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
SHEET_WORKORDERS     = "Workorders"         # history sheet (unchanged)
SHEET_ASSET_MASTER   = "Asset_Master"
SHEET_WO_MASTER      = "Workorders_Master"  # new listing sheet with flags
SHEET_USERS_CANDIDATES = ["Users", "Users]", "USERS", "users"]  # accept minor naming variations

REQUIRED_WO_COLS = [
    "WORKORDER", "TITLE", "STATUS", "PO", "P/N", "QUANTITY RECEIVED",
    "Vendors", "COMPLETED ON", "ASSET", "Location",
]
OPTIONAL_SORT_COL = "Sort"       # if present, 1=WO, 2=PO, 3=TRANS
ASSET_MASTER_COLS = ["Location", "ASSET"]

# The canonical columns we expect in Workorders_Master now:
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
    """Coerce common truthy/falsey strings to booleans; keep NaN -> False by default."""
    if s.dtype == bool:
        return s
    m = s.astype(str).str.strip().str.lower()
    true_vals  = {"true","yes","y","1","t"}
    false_vals = {"false","no","n","0","f","", "nan", "none"}
    out = m.map(lambda x: True if x in true_vals else (False if x in false_vals else False))
    return out.astype(bool)

# ---------- data loaders ----------
@st.cache_data(show_spinner=False)
def load_workorders_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    """History sheet for Asset view."""
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

@st.cache_data(show_spinner=False)
def load_wo_master_df(xlsx_bytes: bytes, sheet: str) -> pd.DataFrame:
    """Listing sheet for Work Orders (non-history) with flags already calculated."""
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,                # read as text, then normalize
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [str(c).strip() for c in df.columns]
    # Keep only the expected columns if present; otherwise keep what's there
    have = [c for c in MASTER_REQUIRED if c in df.columns]
    if have:
        df = df[have].copy()

    # Normalize dates to YYYY-MM-DD strings
    for dc in ("Created on","Planned Start Date","Due date","Started on","Completed on"):
        if dc in df.columns:
            df[dc] = df[dc].map(_norm_date_any)

    # Coerce flags to booleans
    for bc in ("IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"):
        if bc in df.columns:
            df[bc] = coerce_bool(df[bc])

    # Tidy strings
    for c in [x for x in ["ID","Title","Description","Asset","Status","Assigned to","Teams Assigned to","Completed by","Location"] if x in df.columns]:
        df[c] = df[c].astype(str).str.strip()

    # ID often numeric: keep as string for stable display
    if "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()

    return df

@st.cache_data(show_spinner=False)
def load_users_sheet(xlsx_bytes: bytes) -> list[str] | None:
    """Optional Users sheet providing the full picklist of users (names)."""
    for name in SHEET_USERS_CANDIDATES:
        try:
            dfu = pd.read_excel(
                io.BytesIO(xlsx_bytes),
                sheet_name=name,
                dtype=str,
                keep_default_na=False,
                engine="openpyxl",
            )
            # Expect a column named 'User' (case-insensitive)
            cols_low = {c.lower(): c for c in dfu.columns}
            col = cols_low.get("user")
            if not col:
                continue
            users = [u.strip() for u in dfu[col].astype(str).tolist() if str(u).strip()]
            users = sorted(dict.fromkeys(users))  # unique preserve order-ish
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

    # Page choice in SIDEBAR (to leave more width for the table)
    page = st.sidebar.radio("Page", ["🔎 Asset History", "📋 Work Orders"], index=1)

    # Load workbook
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
        # Filters across the top (Location then Asset)
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

        # Load history only if needed
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

        # Order: WORKORDER ASC, then Sort ASC (1=WO,2=PO,3=TRANS), then stable by original row
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

        # Downloads
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

    # ========= Page: Work Orders (listing) =========
    else:
        st.markdown("### Work Orders — Filtered Views (flags from workbook)")

        # Load Workorders_Master + optional Users sheet
        try:
            df_master = load_wo_master_df(xlsx_bytes, SHEET_WO_MASTER)
        except Exception as e:
            st.error(f"Failed to read '{SHEET_WO_MASTER}': {e}")
            st.stop()

        opt_users = load_users_sheet(xlsx_bytes)  # may be None

        # Limit to allowed locations up front
        df_master = df_master[df_master["Location"].isin(allowed_locations)].copy()
        total_in_scope = len(df_master)

        # -------- top-of-page filters --------
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])

        with c1:
            loc_values = sorted(df_master["Location"].dropna().unique().tolist())
            loc_all_label = f"« All my locations ({len(loc_values)}) »"
            chosen_loc = st.selectbox("Location", options=[loc_all_label] + loc_values, index=0)

        # Apply location filter (never broaden beyond allowed_locations)
        if chosen_loc == loc_all_label:
            df_scope = df_master.copy()
        else:
            df_scope = df_master[df_master["Location"] == chosen_loc].copy()

        # Build user list:
        # - If Users sheet exists, use that list.
        # - Otherwise, derive from df_scope (to avoid leaking names outside this location scope).
        with c2:
            if opt_users is not None:
                user_choices = ["— Any user —"] + opt_users
            else:
                derived_users = sorted([u for u in df_scope.get("Assigned to", pd.Series([], dtype=str))
                                        .dropna().astype(str).str.strip().unique().tolist() if u])
                user_choices = ["— Any user —"] + derived_users
            sel_user = st.selectbox("Assigned user", options=user_choices, index=0)

        with c3:
            # Teams: tokenize comma/semicolon from df_scope (in-scope only)
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

        # Apply user filter but do NOT reveal if they have work at other locations
        if sel_user != "— Any user —":
            before = len(df_scope)
            df_scope = df_scope[df_scope["Assigned to"].astype(str).str.strip() == sel_user].copy()
            after = len(df_scope)
            if after == 0:
                # Intentionally vague: doesn't say they have work elsewhere
                st.warning("User not found at this location (or no work orders match).")
                # Show empty table with headers to keep layout stable
                st.dataframe(df_scope, use_container_width=True, hide_index=True)
                st.stop()

        # Apply team filter in-scope only
        if sel_team != "— Any team —":
            def team_hit(s: str) -> bool:
                if not s: return False
                parts = {p.strip() for p in re.split(r"[;,]", str(s)) if p.strip()}
                return sel_team.strip() in parts
            df_scope = df_scope[df_scope["Teams Assigned to"].fillna("").astype(str).map(team_hit)].copy()

        # Bucket by workbook flags
        def pick_view(df_in: pd.DataFrame) -> pd.DataFrame:
            if view == "All" or not {"IsOpen","IsOverdue","IsScheduled","IsCompleted","IsOld"}.issubset(df_in.columns):
                return df_in
            m = {
                "Open": "IsOpen",
                "Overdue": "IsOverdue",
                "Scheduled (Planning)": "IsScheduled",
                "Completed": "IsCompleted",
                "Old": "IsOld",
            }
            col = m.get(view)
            return df_in[df_in[col]].copy() if col else df_in

        df_view = pick_view(df_scope)

        # Choose minimal helpful columns per view
        def present(cols: list[str]) -> list[str]:
            return [c for c in cols if c in df_view.columns]

        cols_all        = present(["ID","Title","Asset","Created on","Planned Start Date","Due date","Started on","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_open       = present(["ID","Title","Asset","Created on","Due date","Assigned to","Teams Assigned to","Location"])
        cols_overdue    = present(["ID","Title","Asset","Due date","Assigned to","Teams Assigned to","Location"])
        cols_sched      = present(["ID","Title","Asset","Planned Start Date","Due date","Assigned to","Teams Assigned to","Location"])
        cols_completed  = present(["ID","Title","Asset","Completed on","Assigned to","Teams Assigned to","Location"])
        cols_old        = present(["ID","Title","Asset","Created on","Due date","Completed on","Assigned to","Teams Assigned to","Location"])

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

        # Sort safely (dates already normalized as text YYYY-MM-DD; pandas sorts lexicographically OK)
        if sort_keys:
            df_view = df_view.sort_values(by=sort_keys, na_position="last")

        st.caption(f"In scope: {total_in_scope}  •  After location/user/team filters: {len(df_scope)}  •  Showing ({view}): {len(df_view)}")
        st.dataframe(df_view[use_cols], use_container_width=True, hide_index=True)

        # Downloads
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


