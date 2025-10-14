# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Page 1: 🔎 Asset History (existing behavior)
# - Page 2: 📋 Work Orders (Location/User/Team -> Open/Overdue/Scheduled/Completed/Old)
# - Uses Workorders sheet for history and Workorders_Master for listing
# - Dates normalized to YYYY-MM-DD
# - Robust GitHub download + "Data last updated" from latest commit
# --------------------------------------------------------------

from __future__ import annotations
import io, textwrap, re
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
from zipfile import BadZipFile

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.13c"

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
SHEET_WO_MASTER      = "Workorders_Master"  # new listing sheet

REQUIRED_WO_COLS = [
    "WORKORDER", "TITLE", "STATUS", "PO", "P/N", "QUANTITY RECEIVED",
    "Vendors", "COMPLETED ON", "ASSET", "Location",
]
OPTIONAL_SORT_COL = "Sort"       # if present, 1=WO, 2=PO, 3=TRANS
ASSET_MASTER_COLS = ["Location", "ASSET"]

# Canonical mapping for Workorders_Master
MASTER_CANON = {
    "WORKORDER": {"id", "workorder", "wo", "wo #", "work order", "work order #", "wo#"},
    "TITLE": {"title"},
    "DESCRIPTION": {"description"},
    "ASSET": {"asset"},
    "STATUS": {"status"},
    "Created On": {"created on", "created", "created date"},
    "Start Date": {"planned start date", "start date", "planned start", "start"},
    "Due Date": {"due date", "due"},
    "Started On": {"started on"},
    "Completed On": {"completed on", "completed"},
    "Assigned To": {"assigned to"},
    "Teams Assigned To": {"teams assigned to"},
    "Completed By": {"completed by"},
    "Location2": {"location2", "location 2", "loc2"},
    "NS Location": {"ns location", "netsuite location", "ns_location"},
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

def _canonize_headers(cols: list[str], canon: dict[str,set[str]]) -> dict[str, str]:
    low = {str(c).strip().lower(): str(c) for c in cols}
    mapping: dict[str, str] = {}
    for key, aliases in canon.items():
        key_low = key.strip().lower()
        if key_low in low:
            mapping[low[key_low]] = key
            continue
        for k_low, orig in low.items():
            k_clean = re.sub(r"\s+", " ", k_low)
            if k_clean in aliases or k_low in aliases:
                mapping[orig] = key
                break
    return mapping

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
    """Listing sheet for Work Orders (non-history)."""
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    # Canonicalize headers
    df.columns = [str(c).strip() for c in df.columns]
    col_map = _canonize_headers(df.columns.tolist(), MASTER_CANON)
    df = df.rename(columns=col_map)

    # Effective Location: prefer Location2, else NS Location
    loc2 = df.get("Location2", "")
    nsl  = df.get("NS Location", "")
    df["Location"] = (
        pd.Series(loc2, dtype="object").astype(str).str.strip()
        .where(pd.Series(loc2, dtype="object").astype(str).str.strip().ne(""),
               pd.Series(nsl, dtype="object").astype(str).str.strip())
    )

    # Normalize dates (as display strings)
    for dc in ("Created On","Start Date","Due Date","Started On","Completed On"):
        if dc in df.columns:
            df[dc] = df[dc].map(_norm_date_any)

    # tidy strings
    for c in [x for x in ["WORKORDER","TITLE","DESCRIPTION","ASSET","Assigned To","Teams Assigned To","Completed By","Location"] if x in df.columns]:
        df[c] = df[c].astype(str).str.strip()

    return df

# ---------- App ----------
st.sidebar.caption(f"SPF Work Orders — v{APP_VERSION}")

cfg = load_config()
cfg = to_plain(cfg)

# ---- Auth (no lingering form) ----
cookie_cfg = cfg.get("cookie", {})
auth = stauth.Authenticate(
    cfg.get("credentials", {}),
    cookie_cfg.get("name", "spf_workorders_portal"),
    cookie_cfg.get("key", "super_secret_key"),
    cookie_cfg.get("expiry_days", 7),
)

_login_ph = st.empty()
with _login_ph.container():
    name, auth_status, username = auth.login("Login", "main")

if auth_status is False:
    st.error("Username/password is incorrect")
    st.stop()
elif auth_status is None:
    st.info("Please log in.")
    st.stop()
else:
    _login_ph.empty()
    auth.logout("Logout", "sidebar")
    st.sidebar.success(f"Logged in as {name}")

    updated = get_data_last_updated()
    if updated:
        st.sidebar.caption(updated)

    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

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
        st.error(f"Failed to read Excel (history): {e}")
        st.stop()

    # Load Workorders_Master (listing) if present
    try:
        df_master = load_wo_master_df(xlsx_bytes, SHEET_WO_MASTER)
    except Exception:
        df_master = None  # we'll warn inside the tab

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

    # --- Tabs (pages) ---
    tab_hist, tab_list = st.tabs(["🔎 Asset History", "📋 Work Orders"])

    # =========================
    # Tab 1: Asset History
    # =========================
    with tab_hist:
        # Sidebar selectors for history
        loc_placeholder = "— Choose Location —"
        loc_options = [loc_placeholder] + sorted(allowed_locations)
        chosen_loc = st.sidebar.selectbox("Location", options=loc_options, index=0)

        assets_for_loc = sorted(df_am.loc[df_am["Location"] == chosen_loc, "ASSET"].dropna().unique().tolist()) if chosen_loc != loc_placeholder else []
        asset_placeholder = "— Choose Asset —"
        asset_options = [asset_placeholder] + assets_for_loc
        chosen_asset = st.sidebar.selectbox("Asset", options=asset_options, index=0)

        st.markdown("### Asset History")
        if chosen_loc == loc_placeholder:
            st.info("Select a Location to load Assets.")
        elif chosen_asset == asset_placeholder:
            st.info("Select an Asset to view its history.")
        else:
            df = df_all[(df_all["Location"] == chosen_loc) & (df_all["ASSET"] == chosen_asset)].copy()

            # Optional: drop negative/zero part transactions (keeps nulls/non-part rows)
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

            # Blank WORKORDER only for Sort in {2,3}
            df.loc[df["__sort_key"].isin([2, 3]), "WORKORDER"] = ""

            # Build display without helpers and without Sort column
            drop_cols = ["__row", "__sort_key"]
            if OPTIONAL_SORT_COL in df.columns:
                drop_cols.append(OPTIONAL_SORT_COL)
            df_out = df.drop(columns=drop_cols, errors="ignore")

            st.markdown(f"**Work Orders — {chosen_loc} — {chosen_asset}**")
            if df_out.empty:
                st.info("No history found for this Asset.")
            st.dataframe(df_out, use_container_width=True, hide_index=True)

            # Downloads
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

    # =========================
    # Tab 2: Work Orders (listing) — date-driven buckets, STATUS ignored
    # =========================
    with tab_list:
        st.markdown("### Work Orders — Filtered Views (date-driven)")

        if df_master is None:
            st.warning(f"Sheet '{SHEET_WO_MASTER}' not found. Add it to the workbook to enable this page.")
        else:
            # Scope by exact Location (df_master['Location'] is already Location2→fallback NS Location)
            loc_values_in_master = sorted(set(df_master["Location"].dropna().astype(str)))
            visible_locs = sorted(set(loc_values_in_master).intersection(set(allowed_locations)))

            loc_all_label = f"« All my locations ({len(visible_locs)}) »"
            loc_options2 = [loc_all_label] + visible_locs
            chosen_loc2 = st.selectbox("Location scope", options=loc_options2, index=0)

            if chosen_loc2 == loc_all_label:
                df_scope = df_master[df_master["Location"].isin(allowed_locations)].copy()
            else:
                df_scope = df_master[df_master["Location"] == chosen_loc2].copy()

            # ---------- Optional: allowed users/teams from 'WO_Assignees' sheet ----------
            assignees_users, assignees_teams = None, None
            try:
                df_assign = pd.read_excel(
                    io.BytesIO(xlsx_bytes),
                    sheet_name="WO_Assignees",
                    dtype=str,
                    keep_default_na=False,
                    engine="openpyxl",
                )
                df_assign.columns = [str(c).strip() for c in df_assign.columns]
                cols_low = {c.lower(): c for c in df_assign.columns}
                col_loc  = cols_low.get("location")
                col_user = cols_low.get("assigned to") or cols_low.get("user") or cols_low.get("username")
                col_team = cols_low.get("team") or cols_low.get("teams")
                if col_loc and (col_user or col_team):
                    if chosen_loc2 == loc_all_label:
                        df_a = df_assign[df_assign[col_loc].astype(str).isin(visible_locs)]
                    else:
                        df_a = df_assign[df_assign[col_loc].astype(str) == str(chosen_loc2)]
                    if col_user:
                        assignees_users = sorted(
                            u for u in df_a[col_user].dropna().astype(str).str.strip().unique().tolist() if u
                        )
                    if col_team:
                        assignees_teams = sorted(
                            t for t in df_a[col_team].dropna().astype(str).str.strip().unique().tolist() if t
                        )
            except Exception:
                pass

            # ---------- User / Team filters (fallbacks if WO_Assignees missing) ----------
            fallback_users = sorted(
                u for u in (df_scope["Assigned To"] if "Assigned To" in df_scope.columns else pd.Series([], dtype=str))
                    .dropna().astype(str).str.strip().unique().tolist() if u
            )
            users = assignees_users if assignees_users is not None else fallback_users
            sel_users = st.multiselect("Filter: Assigned User(s)", options=users, default=[])
            if sel_users and "Assigned To" in df_scope.columns:
                df_scope = df_scope[df_scope["Assigned To"].astype(str).str.strip().isin(sel_users)].copy()

            # Teams (tokenize comma/semicolon if needed)
            if assignees_teams is not None:
                teams = assignees_teams
            else:
                raw_teams = (df_scope["Teams Assigned To"] if "Teams Assigned To" in df_scope.columns else pd.Series([], dtype=str)).fillna("").astype(str)
                token_set = set()
                for v in raw_teams:
                    for t in re.split(r"[;,]", v):
                        t = t.strip()
                        if t:
                            token_set.add(t)
                teams = sorted(token_set)

            sel_teams = st.multiselect("Filter: Team(s)", options=teams, default=[])
            if sel_teams and "Teams Assigned To" in df_scope.columns:
                sel_teams_norm = {t.strip() for t in sel_teams}
                def team_hit(s: str) -> bool:
                    if not s: return False
                    parts = {p.strip() for p in re.split(r"[;,]", str(s)) if p.strip()}
                    return bool(parts & sel_teams_norm)
                df_scope = df_scope[df_scope["Teams Assigned To"].fillna("").astype(str).map(team_hit)].copy()

            # ---------- Dates (vectorized) ----------
            def s(col: str):
                return df_scope[col] if col in df_scope.columns else pd.Series(pd.NA, index=df_scope.index)

            created_dt   = pd.to_datetime(s("Created On"),   errors="coerce")
            start_dt     = pd.to_datetime(s("Start Date"),   errors="coerce")
            due_dt       = pd.to_datetime(s("Due Date"),     errors="coerce")
            completed_dt = pd.to_datetime(s("Completed On"), errors="coerce")

            today = pd.Timestamp.today().normalize()

            # ---------- Buckets (STATUS ignored) ----------
            is_completed = completed_dt.notna()
            is_overdue   = (~is_completed) & due_dt.notna()   & (due_dt < today)
            is_sched     = (~is_completed) & start_dt.notna() & (start_dt > today)
            is_open      = (~is_completed) & (~is_overdue) & (~is_sched)
            is_old_flag  = (~is_completed) & created_dt.notna() & (created_dt < today)

            # ---------- Column sets per view ----------
            def present(cols: list[str]) -> list[str]:
                return [c for c in cols if c in df_scope.columns]

            cols_all     = present(["WORKORDER","TITLE","ASSET","Created On","Start Date","Due Date","Completed On","Assigned To","Teams Assigned To","Location"])
            cols_open    = present(["WORKORDER","TITLE","ASSET","Created On","Due Date","Assigned To","Teams Assigned To","Location"])
            cols_overdue = present(["WORKORDER","TITLE","ASSET","Due Date","Assigned To","Teams Assigned To","Location"])
            cols_sched   = present(["WORKORDER","TITLE","ASSET","Start Date","Due Date","Assigned To","Teams Assigned To","Location"])
            cols_done    = present(["WORKORDER","TITLE","ASSET","Completed On","Assigned To","Teams Assigned To","Location"])
            cols_old     = present(["WORKORDER","TITLE","ASSET","Created On","Due Date","Completed On","Assigned To","Teams Assigned To","Location"])

            # ---------- Presenter ----------
            def show(df_in: pd.DataFrame, label: str, cols: list[str], sort_keys: list[str] | None = None):
                st.markdown(f"**{label} — rows: {len(df_in)}**")
                if df_in.empty:
                    st.info("No rows.")
                    return
                if sort_keys:
                    df_in = df_in.copy()
                    df_in.sort_values(by=[k for k in sort_keys if k in df_in.columns], inplace=True)
                st.dataframe(df_in[cols] if cols else df_in, use_container_width=True, hide_index=True)
                c1, c2, _ = st.columns([1,1,3])
                with c1:
                    st.download_button(
                        "⬇️ Excel (.xlsx)",
                        data=to_xlsx_bytes(df_in[cols] if cols else df_in, sheet=label.replace(" ","_")),
                        file_name=f"WO_{label.replace(' ','_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                with c2:
                    st.download_button(
                        "⬇️ Word (.docx)",
                        data=to_docx_bytes(df_in[cols] if cols else df_in, title=f"Work Orders — {label}"),
                        file_name=f"WO_{label.replace(' ','_')}.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )

            # ---------- Tabs ----------
            t_all, t_open, t_overdue, t_sched, t_done, t_old = st.tabs(
                ["All (in scope)", "Open (Today)", "Overdue", "Scheduled (Planning)", "Completed", "Old (flag)"]
            )

            with t_all:
                show(df_scope.copy(), "All (in scope)", cols_all, sort_keys=["Completed On","Due Date","Start Date","Created On"])

            with t_open:
                show(df_scope[is_open].copy(), "Open (Today)", cols_open, sort_keys=["Due Date","Created On"])

            with t_overdue:
                show(df_scope[is_overdue].copy(), "Overdue", cols_overdue, sort_keys=["Due Date"])

            with t_sched:
                show(df_scope[is_sched].copy(), "Scheduled (Planning)", cols_sched, sort_keys=["Start Date","Due Date"])

            with t_done:
                show(df_scope[is_completed].copy(), "Completed", cols_done, sort_keys=["Completed On"])

            with t_old:
                show(df_scope[is_old_flag].copy(), "Old (flag)", cols_old, sort_keys=["Created On","Due Date"])

