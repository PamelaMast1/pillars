# pages/05_UserPreferences_MultiSource.py
import os
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

st.set_page_config(page_title="User Preferences by Data Source", layout="wide")

# -----------------------------
# Connection (Windows Auth-friendly)
# -----------------------------
def _build_sqlalchemy_url():
    cfg = st.secrets["sqlserver"]
    driver = cfg.get("driver", "ODBC Driver 18 for SQL Server")
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={cfg['server']}",
        f"DATABASE={cfg['database']}",
        f"Encrypt={cfg.get('encrypt','no')}",
        f"TrustServerCertificate={cfg.get('trust_server_certificate','yes')}",
    ]
    if cfg.get("windows_auth", True):
        parts.append("Trusted_Connection=yes")  # Integrated Security
    else:
        parts += [f"UID={cfg['username']}", f"PWD={cfg['password']}"]
    odbc = ";".join(parts) + ";"
    return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc)

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(_build_sqlalchemy_url(), pool_pre_ping=True, fast_executemany=True)

engine = get_engine()

# -----------------------------
# Schema bootstrap (idempotent)
# Keeps simple string-based dataSource (no extra table required)
# -----------------------------
def init_schema():
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[userNodePreference]') AND type in (N'U'))
    BEGIN
        CREATE TABLE dbo.userNodePreference (
            userName            varchar(200) NOT NULL,
            pillarNode_id       int NOT NULL,
            pillarNodeValue_id  int NOT NULL,
            dataSource          varchar(100) NOT NULL CONSTRAINT DF_userNodePreference_dataSource DEFAULT(''),
            dateAdded           datetime NOT NULL CONSTRAINT DF_userNodePreference_dateAdded DEFAULT (GETDATE()),
            CONSTRAINT PK_userNodePreference PRIMARY KEY CLUSTERED (userName, pillarNode_id, dataSource)
        );
        ALTER TABLE dbo.userNodePreference WITH CHECK
            ADD CONSTRAINT FK_userPref_Node  FOREIGN KEY(pillarNode_id)      REFERENCES dbo.pillarNode(id);
        ALTER TABLE dbo.userNodePreference WITH CHECK
            ADD CONSTRAINT FK_userPref_Value FOREIGN KEY(pillarNodeValue_id) REFERENCES dbo.pillarNodeValue(id);

        CREATE INDEX IX_userNodePreference_user_ds ON dbo.userNodePreference(userName, dataSource);
    END
    """
    with engine.begin() as cx:
        cx.execute(text(ddl))

init_schema()

# -----------------------------
# Data helpers
# -----------------------------
def fetch_categories() -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text("SELECT id, category FROM dbo.category ORDER BY category;"), cx)

def fetch_subcategories(category_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(
            text("""SELECT id, subCategory
                    FROM dbo.subCategory
                    WHERE category_id = :cid
                    ORDER BY subCategory;"""),
            cx, params={"cid": category_id}
        )

def fetch_nodes(subcat_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(
            text("""SELECT id, pillarNode, pillarNodeDescription
                    FROM dbo.pillarNode
                    WHERE subCategory_id = :sid
                    ORDER BY pillarNode;"""),
            cx, params={"sid": subcat_id}
        )

def fetch_values_for_node(node_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(
            text("""SELECT v.id, v.pillarNodeValue, v.pillarNodeValueDescription
                    FROM dbo.pillarNodeValueMapping m
                    JOIN dbo.pillarNodeValue v ON v.id = m.pillarNodeValue_id
                    WHERE m.pillarNode_id = :nid
                    ORDER BY v.pillarNodeValue;"""),
            cx, params={"nid": node_id}
        )

def fetch_user_contexts(user_name: str) -> List[str]:
    with engine.begin() as cx:
        rows = cx.execute(
            text("""SELECT DISTINCT dataSource
                    FROM dbo.userNodePreference
                    WHERE userName = :u
                    ORDER BY dataSource;"""),
            {"u": user_name}
        ).fetchall()
    return [r[0] for r in rows if r and r[0] is not None]

def fetch_user_pref_map(user_name: str, data_source: str) -> Dict[int, int]:
    with engine.begin() as cx:
        rows = cx.execute(
            text("""SELECT pillarNode_id, pillarNodeValue_id
                    FROM dbo.userNodePreference
                    WHERE userName = :u AND dataSource = :ds;"""),
            {"u": user_name, "ds": data_source}
        ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}

def upsert_pref(user_name: str, data_source: str, node_id: int, value_id: int):
    with engine.begin() as cx:
        cx.execute(
            text("""
            MERGE dbo.userNodePreference AS tgt
            USING (SELECT :u AS userName, :nid AS pillarNode_id, :ds AS dataSource) AS src
            ON (tgt.userName = src.userName AND tgt.pillarNode_id = src.pillarNode_id AND tgt.dataSource = src.dataSource)
            WHEN MATCHED THEN UPDATE
                SET pillarNodeValue_id = :vid, dateAdded = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (userName, pillarNode_id, pillarNodeValue_id, dataSource)
                VALUES (:u, :nid, :vid, :ds);
            """),
            {"u": user_name, "ds": data_source, "nid": int(node_id), "vid": int(value_id)}
        )

def clear_pref(user_name: str, data_source: str, node_id: int):
    with engine.begin() as cx:
        cx.execute(
            text("""DELETE FROM dbo.userNodePreference
                    WHERE userName = :u AND dataSource = :ds AND pillarNode_id = :nid;"""),
            {"u": user_name, "ds": data_source, "nid": int(node_id)}
        )

def clear_all_prefs(user_name: str, data_source: str):
    with engine.begin() as cx:
        cx.execute(
            text("""DELETE FROM dbo.userNodePreference
                    WHERE userName = :u AND dataSource = :ds;"""),
            {"u": user_name, "ds": data_source}
        )

def rename_context(user_name: str, old: str, new: str):
    with engine.begin() as cx:
        cx.execute(
            text("""UPDATE dbo.userNodePreference
                    SET dataSource = :new
                    WHERE userName = :u AND dataSource = :old;"""),
            {"u": user_name, "old": old, "new": new}
        )

def duplicate_context(user_name: str, src: str, dest: str):
    # Copy/overwrite prefs from src -> dest
    with engine.begin() as cx:
        cx.execute(
            text("""
            MERGE dbo.userNodePreference AS tgt
            USING (
                SELECT :u AS userName, p.pillarNode_id, :dest AS dataSource, p.pillarNodeValue_id
                FROM dbo.userNodePreference p
                WHERE p.userName = :u AND p.dataSource = :src
            ) AS srcq
            ON (tgt.userName = srcq.userName AND tgt.pillarNode_id = srcq.pillarNode_id AND tgt.dataSource = srcq.dataSource)
            WHEN MATCHED THEN UPDATE
                SET pillarNodeValue_id = srcq.pillarNodeValue_id, dateAdded = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (userName, pillarNode_id, pillarNodeValue_id, dataSource)
                VALUES (srcq.userName, srcq.pillarNode_id, srcq.pillarNodeValue_id, srcq.dataSource);
            """),
            {"u": user_name, "src": src, "dest": dest}
        )

def fetch_current_prefs_table(user_name: str, data_source: str) -> pd.DataFrame:
    sql = text("""
        SELECT c.category,
               s.subCategory,
               n.pillarNode,
               v.pillarNodeValue AS selectedValue,
               p.dateAdded
        FROM dbo.userNodePreference p
        JOIN dbo.pillarNode n ON n.id = p.pillarNode_id
        JOIN dbo.pillarNodeValue v ON v.id = p.pillarNodeValue_id
        JOIN dbo.subCategory s ON s.id = n.subCategory_id
        JOIN dbo.category c ON c.id = s.category_id
        WHERE p.userName = :u AND p.dataSource = :ds
        ORDER BY c.category, s.subCategory, n.pillarNode;
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params={"u": user_name, "ds": data_source})

# -----------------------------
# UI: User + Data Source Manager
# -----------------------------
st.title("👤 User Preferences by Data Source")
st.caption("Create/choose a **Data Source** (e.g., “Salesforce API”, “Workday API”, “Internal SQL A”), then pick values for each Pillar Node. One selection per node per source.")

with st.sidebar:
    st.header("Profile & Sources")
    default_user = os.getenv("USERNAME") or os.getenv("USER") or "me"
    user_name = st.text_input("User name", value=st.session_state.get("pref_user", default_user))
    st.session_state["pref_user"] = user_name.strip()

    st.markdown("---")
    st.subheader("Manage Data Sources")
    existing_sources = fetch_user_contexts(user_name) if user_name else []
    selected_source = st.selectbox(
        "Select source",
        options=(existing_sources if existing_sources else []),
        index=0 if existing_sources else None,
        placeholder="Pick a source…",
    )

    new_source_name = st.text_input("New source name", placeholder="e.g., Salesforce API")
    cA, cB = st.columns(2)
    with cA:
        if st.button("➕ Create/Select"):
            # No row needed yet; simply switch context
            st.session_state["active_source"] = (new_source_name or selected_source or "").strip()
            if not st.session_state["active_source"]:
                st.warning("Enter a new source or pick an existing one.")
            else:
                st.success(f"Active source: {st.session_state['active_source']}")
                st.rerun()
    with cB:
        if selected_source and st.button("🗑️ Delete source"):
            clear_all_prefs(user_name, selected_source)
            st.success(f"Deleted all preferences for source '{selected_source}'.")
            if st.session_state.get("active_source") == selected_source:
                st.session_state["active_source"] = ""
            st.rerun()

    if selected_source:
        new_name = st.text_input("Rename selected to", value=selected_source, key="rename_box")
        cC, cD = st.columns(2)
        with cC:
            if new_name and new_name != selected_source and st.button("✏️ Rename"):
                rename_context(user_name, selected_source, new_name)
                if st.session_state.get("active_source") == selected_source:
                    st.session_state["active_source"] = new_name
                st.success(f"Renamed '{selected_source}' → '{new_name}'.")
                st.rerun()
        with cD:
            dup_to = st.text_input("Duplicate selected to", placeholder="e.g., Workday API")
            if dup_to and st.button("📄 Duplicate"):
                duplicate_context(user_name, selected_source, dup_to.strip())
                st.success(f"Duplicated '{selected_source}' → '{dup_to}'.")
                st.session_state["active_source"] = dup_to.strip()
                st.rerun()

# Determine active source
if not user_name:
    st.info("Enter a user name to continue.")
    st.stop()

active_source = st.session_state.get("active_source", "")
if not active_source:
    st.info("Pick an existing source (left) or enter a new **New source name** and click **Create/Select**.")
    st.stop()

st.write(f"**User:** `{user_name}` · **Active Source:** `{active_source}`")

# -----------------------------
# Filters
# -----------------------------
with st.expander("🔎 Filters", expanded=False):
    filter_text = st.text_input("Filter nodes/values", placeholder="Type to filter by node or value…").strip().lower()

# -----------------------------
# Drilldown + Editor (same pattern as before)
# -----------------------------
pref_map = fetch_user_pref_map(user_name, active_source)
cat_df = fetch_categories()
if cat_df.empty:
    st.info("No categories found. Add categories first.")
    st.stop()

st.markdown("---")
changes: List[Dict] = []

for cat in cat_df.itertuples():
    sub_df = fetch_subcategories(int(cat.id))
    if sub_df.empty:
        continue
    with st.expander(f"📁 {cat.category}", expanded=False):
        for sub in sub_df.itertuples():
            nodes_df = fetch_nodes(int(sub.id))
            if nodes_df.empty:
                continue
            st.markdown(f"### 🧩 {sub.subCategory}")

            cols = st.columns(2, gap="large")
            col_idx = 0

            for node in nodes_df.itertuples():
                node_text = f"{node.pillarNode} {(node.pillarNodeDescription or '')}".lower()
                values_df = fetch_values_for_node(int(node.id))
                if filter_text:
                    if (filter_text not in node_text) and (
                        values_df.empty or not (
                            values_df["pillarNodeValue"].str.lower().str.contains(filter_text).any()
                            | values_df["pillarNodeValueDescription"].fillna("").str.lower().str.contains(filter_text).any()
                        )
                    ):
                        continue
                    # filter values
                    values_df = values_df[
                        values_df["pillarNodeValue"].str.lower().str.contains(filter_text)
                        | values_df["pillarNodeValueDescription"].fillna("").str.lower().str.contains(filter_text)
                    ]

                with cols[col_idx]:
                    with st.container(border=True):
                        st.markdown(f"**{node.pillarNode}**")
                        if node.pillarNodeDescription:
                            st.caption(node.pillarNodeDescription)

                        if values_df.empty:
                            st.info("No mapped values (or filtered out).")
                            sel = None
                        else:
                            labels = [f"{r.pillarNodeValue}" for r in values_df.itertuples()]
                            ids = [int(r.id) for r in values_df.itertuples()]
                            options = [("— N/A —", None)] + list(zip(labels, ids))

                            current_vid = pref_map.get(int(node.id))
                            default_index = 0
                            if current_vid and current_vid in ids:
                                default_index = 1 + ids.index(current_vid)

                            choice = st.selectbox(
                                "Select value",
                                options=options,
                                index=default_index,
                                format_func=lambda t: t[0],
                                key=f"sel_{cat.id}_{sub.id}_{node.id}",
                            )
                            sel = choice[1] if isinstance(choice, tuple) else None

                        changes.append({"node_id": int(node.id), "value_id": sel})

                col_idx = 1 - col_idx

st.markdown("---")
c1, c2, c3 = st.columns([1,1,1])
with c1:
    if st.button("💾 Save selections", type="primary"):
        updated, cleared = 0, 0
        for ch in changes:
            nid, vid = ch["node_id"], ch["value_id"]
            if vid is None:
                if pref_map.get(nid) is not None:
                    clear_pref(user_name, active_source, nid)
                    cleared += 1
            else:
                upsert_pref(user_name, active_source, nid, vid)
                updated += 1
        st.success(f"Saved. Updated {updated}, cleared {cleared}.")
        st.rerun()
with c2:
    if st.button("↩️ Revert (reload)"):
        st.rerun()
with c3:
    if st.button("🗑️ Clear ALL in this source"):
        clear_all_prefs(user_name, active_source)
        st.success(f"Cleared all preferences for '{active_source}'.")
        st.rerun()

# -----------------------------
# Current selections table
# -----------------------------
st.subheader("Current selections for this source")
cur = fetch_current_prefs_table(user_name, active_source)
if cur.empty:
    st.info("No selections stored yet for this source.")
else:
    st.dataframe(cur, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(f"Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
