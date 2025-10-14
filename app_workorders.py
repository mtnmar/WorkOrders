# app_workorders.py
# --------------------------------------------------------------
# SPF Work Orders (reads Excel from GitHub private repo)
# - Login via streamlit-authenticator
# - Access control by Location (user -> allowed locations)
# - Page 1: 🔎 Asset History (existing behavior)
# - Page 2: 📋 Work Orders (Location/User/Team -> Open/Overdue/Not Started/Old)
# - Uses Workorders sheet for history and Workorders_Master for listing
# - Dates normalized to YYYY-MM-DD
# - Robust GitHub download + "Data last updated" from latest commit
# --------------------------------------------------------------

from __future__ import annotations
import io, textwrap, re
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone, timedelta
from zipfile import BadZipFile

import pandas as pd
import streamlit as st
import yaml

APP_VERSION = "2025.10.13"

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

# Status buckets
OPEN_STATUSES = {"OPEN", "ON HOLD", "ON-HOLD", "IN PROCESS", "IN-PROCESS", "INPROCESS", "PENDING"}
DONE_STATUSES = {"COMPLETE", "COMPLETED", "CLOSED", "DONE", "RESOLVED"}

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
        # exact header
        if key_low in low:
            mapping[low[key_low]] = key
            continue
        # alias match
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

    # Status upper for bucketing
    if "STATUS" in df.columns:
        df["__STATUS_UP"] = df["STATUS"].map(lambda s: re.sub(r"\s+", " ", str(s).strip().upper()))
    else:
        df["__STATUS_UP"] = ""

    # tidy strings
    for c in [x for x in ["WORKORDER","TITLE","DESCRIPTION","ASSET","Assigned To","Teams Assigned To","Completed By","Location"] if x in df.columns]:
        df[c] = df[c].astype(str).str.strip()

    return df

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
    # Tab 2: Work Orders (listing)
    # =========================
    with tab_list:
        st.markdown("### Work Orders — Filtered Views")

        if df_master is None:
            st.warning(f"Sheet '{SHEET_WO_MASTER}' not found. Add it to the workbook to enable this page.")
            st.stop()

        # Allowed locations options (from Asset Master)
        loc_all_label = "« All my locations »"
        loc_options2 = [loc_all_label] + sorted(allowed_locations)
        chosen_loc2 = st.selectbox("Location scope", options=loc_options2, index=0)

        # Scope: by Location, by Assigned User, by Team
        scope = st.radio("Scope", ["By Location", "Assigned User", "Team"], horizontal=True, index=0)

        # Filter df_master by allowed locations first (Location column already computed: Location2 else NS Location)
        if chosen_loc2 == loc_all_label:
            df_scope = df_master[df_master["Location"].isin(allowed_locations)].copy()
        else:
            df_scope = df_master[df_master["Location"] == chosen_loc2].copy()

        # Build pickers for user/team within the chosen location set
        if scope == "Assigned User":
            users = sorted([u for u in df_scope.get("Assigned To", pd.Series([], dtype=str)).dropna().astype(str).str.strip().unique().tolist() if u])
            user_sel = st.selectbox("Assigned To", options=(["— All users —"] + users), index=0)
            if user_sel != "— All users —":
                df_scope = df_scope[df_scope["Assigned To"].astype(str).str.strip() == user_sel]
        elif scope == "Team":
            # Teams could be comma/semicolon separated; build list
            raw = df_scope.get("Teams Assigned To", pd.Series([], dtype=str)).fillna("").astype(str)
            split_vals = set()
            for v in raw:
                for p in re.split(r"[;,]", v):
                    p = p.strip()
                    if p:
                        split_vals.add(p)
            teams = sorted(split_vals)
            team_sel = st.selectbox("Team", options=(["— All teams —"] + teams), index=0)
            if team_sel != "— All teams —":
                df_scope = df_scope[df_scope["Teams Assigned To"].astype(str).str.contains(rf"(?:^|[;,]\s*){re.escape(team_sel)}(?:\s*[;,]|$)", na=False)]

        # Compute date objects for bucketing
        today = datetime.now().date()
        def to_date_or_none(s: str):
            s = _norm_date_any(s)
            return None if not s else datetime.strptime(s, "%Y-%m-%d").date()
        created = df_scope["Created On"].map(to_date_or_none) if "Created On" in df_scope.columns else pd.Series([None]*len(df_scope), index=df_scope.index)
        start   = df_scope["Start Date"].map(to_date_or_none)   if "Start Date"   in df_scope.columns else pd.Series([None]*len(df_scope), index=df_scope.index)
        due     = df_scope["Due Date"].map(to_date_or_none)     if "Due Date"     in df_scope.columns else pd.Series([None]*len(df_scope), index=df_scope.index)
        started = df_scope["Started On"].map(to_date_or_none)   if "Started On"   in df_scope.columns else pd.Series([None]*len(df_scope), index=df_scope.index)
        done    = df_scope["Completed On"].map(to_date_or_none) if "Completed On" in df_scope.columns else pd.Series([None]*len(df_scope), index=df_scope.index)
        statusU = df_scope["__STATUS_UP"] if "__STATUS_UP" in df_scope.columns else pd.Series([""]*len(df_scope), index=df_scope.index)

        # Buckets
        is_completed = done.notna() | statusU.isin(DONE_STATUSES)
        is_overdue   = (~is_completed) & due.notna() & (due < today)
        # "Open": not completed and in open statuses (regardless of start date)
        is_open      = (~is_completed) & statusU.isin(OPEN_STATUSES)
        # "Not Started": not completed and not yet started (Started On missing AND (Start Date missing or in future))
        is_not_started = (~is_completed) & started.isna() & ((start.isna()) | (start > today))

        # Old threshold
        old_days = st.slider("Old threshold (days since Created On)", min_value=15, max_value=120, value=45, step=5)
        is_old   = (~is_completed) & created.notna() & ((today - created).dt.days >= old_days)

        # Tabs within listing
        t_open, t_overdue, t_not_started, t_old = st.tabs(["Open", "Overdue", "Not Started", f"Old (≥ {old_days}d)"])

        def present(cols: list[str]) -> list[str]:
            return [c for c in cols if c in df_scope.columns]

        # Column sets
        cols_open = present(["WORKORDER","TITLE","ASSET","STATUS","Created On","Start Date","Due Date","Assigned To","Teams Assigned To"])
        cols_overdue = present(["WORKORDER","TITLE","ASSET","STATUS","Due Date","Assigned To","Teams Assigned To"])
        cols_not_started = present(["WORKORDER","TITLE","ASSET","STATUS","Start Date","Due Date","Assigned To","Teams Assigned To"])
        cols_old = present(["WORKORDER","TITLE","ASSET","STATUS","Created On","Start Date","Due Date","Assigned To","Teams Assigned To"])

        def show(df_in: pd.DataFrame, label: str, cols: list[str], sort_keys: list[str] | None = None):
            st.markdown(f"**{label} — rows: {len(df_in)}**")
            if df_in.empty:
                st.info("No rows.")
                return
            if sort_keys:
                df_in = df_in.copy()
                # normalize sort keys to sortable strings (YYYY-MM-DD already)
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

        with t_open:
            df_open = df_scope[is_open].copy()
            show(df_open, "Open", cols_open, sort_keys=["Due Date","Start Date","Created On"])

        with t_overdue:
            df_od = df_scope[is_overdue].copy()
            show(df_od, "Overdue", cols_overdue, sort_keys=["Due Date"])

        with t_not_started:
            df_ns = df_scope[is_not_started].copy()
            show(df_ns, "Not Started", cols_not_started, sort_keys=["Start Date","Due Date"])

        with t_old:
            df_old = df_scope[is_old].copy()
            # add Age (days) for context if Created On exists
            if "Created On" in df_old.columns:
                def _age_days(v: str) -> int | None:
                    s = _norm_date_any(v)
                    if not s: return None
                    d = datetime.strptime(s, "%Y-%m-%d").date()
                    return (today - d).days
                df_old = df_old.copy()
                df_old["Age (days)"] = df_old["Created On"].map(_age_days)
                if "Age (days)" not in cols_old:
                    cols_old = cols_old + ["Age (days)"]
            show(df_old, f"Old (≥ {old_days}d)", cols_old, sort_keys=["Created On","Due Date"])





