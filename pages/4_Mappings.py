# pages/04_NodeValueMapping.py
import urllib.parse
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

st.set_page_config(page_title="Pillar Node ↔ Value Mapping", layout="wide")

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
# Data helpers
# -----------------------------
def fetch_categories():
    sql = text("SELECT id, category FROM dbo.category ORDER BY category;")
    with engine.begin() as cx:
        return pd.read_sql(sql, cx)

def fetch_subcategories(category_id: int):
    sql = text("""
        SELECT id, subCategory
        FROM dbo.subCategory
        WHERE category_id = :cid
        ORDER BY subCategory;
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params={"cid": category_id})

def fetch_pillar_nodes(subcat_id: int):
    sql = text("""
        SELECT id, pillarNode, pillarNodeDescription, dateAdded
        FROM dbo.pillarNode
        WHERE subCategory_id = :sid
        ORDER BY pillarNode;
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params={"sid": subcat_id})

def fetch_mapped_values(node_id: int, search: str | None = None):
    sql = """
        SELECT v.id,
               v.pillarNodeValue,
               v.pillarNodeValueDescription,
               m.dateAdded
        FROM dbo.pillarNodeValueMapping m
        JOIN dbo.pillarNodeValue v
          ON v.id = m.pillarNodeValue_id
        WHERE m.pillarNode_id = :nid
    """
    params = {"nid": node_id}
    if search:
        sql += " AND (v.pillarNodeValue LIKE :pat OR v.pillarNodeValueDescription LIKE :pat) "
        params["pat"] = f"%{search}%"
    sql += " ORDER BY v.pillarNodeValue;"
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx, params=params)

def fetch_available_values(node_id: int, search: str | None = None):
    # All values NOT currently mapped to this node
    sql = """
        SELECT v.id,
               v.pillarNodeValue,
               v.pillarNodeValueDescription
        FROM dbo.pillarNodeValue v
        LEFT JOIN dbo.pillarNodeValueMapping m
          ON m.pillarNodeValue_id = v.id
         AND m.pillarNode_id = :nid
        WHERE m.pillarNode_id IS NULL
    """
    params = {"nid": node_id}
    if search:
        sql += " AND (v.pillarNodeValue LIKE :pat OR v.pillarNodeValueDescription LIKE :pat) "
        params["pat"] = f"%{search}%"
    sql += " ORDER BY v.pillarNodeValue;"
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx, params=params)

def add_mappings(node_id: int, value_ids: list[int]) -> tuple[int, list[int]]:
    """Insert mappings; ignore ones that already exist. Returns (inserted_count, skipped_ids)."""
    inserted = 0
    skipped = []
    if not value_ids:
        return 0, skipped
    with engine.begin() as cx:
        for vid in value_ids:
            try:
                cx.execute(
                    text("""
                        IF NOT EXISTS (
                          SELECT 1 FROM dbo.pillarNodeValueMapping
                          WHERE pillarNode_id = :nid AND pillarNodeValue_id = :vid
                        )
                        INSERT INTO dbo.pillarNodeValueMapping (pillarNode_id, pillarNodeValue_id)
                        VALUES (:nid, :vid);
                    """),
                    {"nid": node_id, "vid": int(vid)},
                )
                # We can't easily know if it inserted vs skipped without OUTPUT; do a small check:
                check = cx.execute(
                    text("""
                        SELECT COUNT(*) AS c FROM dbo.pillarNodeValueMapping
                        WHERE pillarNode_id = :nid AND pillarNodeValue_id = :vid
                    """),
                    {"nid": node_id, "vid": int(vid)}
                ).fetchone()
                if check and int(check[0]) >= 1:
                    # naive: treat as success (but we can't distinguish brand-new vs pre-existing cheaply)
                    inserted += 1  # count as mapped
            except DBAPIError:
                skipped.append(int(vid))
    # Adjust 'inserted' to count unique final mappings only:
    inserted = len(set(value_ids)) - len(skipped)
    return inserted, skipped

def remove_mappings(node_id: int, value_ids: list[int]) -> int:
    """Delete mappings for given ids. Returns count attempted (approx)."""
    if not value_ids:
        return 0
    with engine.begin() as cx:
        for vid in value_ids:
            cx.execute(
                text("""
                    DELETE FROM dbo.pillarNodeValueMapping
                    WHERE pillarNode_id = :nid AND pillarNodeValue_id = :vid
                """),
                {"nid": node_id, "vid": int(vid)},
            )
    return len(value_ids)

# -----------------------------
# UI
# -----------------------------
st.title("🔗 Map Values to Pillar Nodes (SQL Server)")
st.caption("Associate **pillarNodeValue** entries to a **pillarNode** via `dbo.pillarNodeValueMapping`.")

# Step 1: Category
cat_df = fetch_categories()
if cat_df.empty:
    st.info("No categories found. Add categories first.")
    st.stop()

cat_options = {f"{r.category} (id={int(r.id)})": int(r.id) for r in cat_df.itertuples()}
cat_label = st.selectbox("Category", list(cat_options.keys()))
cat_id = cat_options[cat_label]

# Step 2: Subcategory
sub_df = fetch_subcategories(cat_id)
if sub_df.empty:
    st.info("No subcategories under this category. Add some first.")
    st.stop()

sub_options = {f"{r.subCategory} (id={int(r.id)})": int(r.id) for r in sub_df.itertuples()}
sub_label = st.selectbox("Subcategory", list(sub_options.keys()))
sub_id = sub_options[sub_label]

# Step 3: Pillar Node
node_df = fetch_pillar_nodes(sub_id)
if node_df.empty:
    st.info("No pillar nodes under this subcategory. Add some first.")
    st.stop()

node_options = {f"{r.pillarNode} (id={int(r.id)})": int(r.id) for r in node_df.itertuples()}
node_label = st.selectbox("Pillar Node", list(node_options.keys()))
node_id = node_options[node_label]

st.markdown("---")

# Two-pane mapping manager
left, right = st.columns(2, gap="large")

with left:
    st.subheader("Available values (not mapped)")
    search_available = st.text_input("Filter available values", key="search_avail", placeholder="Type to filter…")
    available_df = fetch_available_values(node_id, search_available.strip() or None)

    if available_df.empty:
        st.info("No unmapped values match the filter.")
        to_add_ids = []
    else:
        labels_avail = [f"{r.pillarNodeValue} (id={int(r.id)})" for r in available_df.itertuples()]
        ids_avail = [int(r.id) for r in available_df.itertuples()]
        choices = list(zip(labels_avail, ids_avail))
        to_add = st.multiselect(
            "Select values to ADD",
            options=choices,
            format_func=lambda t: t[0],
            key="to_add_multiselect",
        )
        to_add_ids = [t[1] for t in to_add]

    c1, c2 = st.columns([1,1])
    with c1:
        if st.button("➕ Add selected", type="primary", use_container_width=True, disabled=(len(to_add_ids) == 0)):
            inserted, skipped = add_mappings(node_id, to_add_ids)
            st.success(f"Mapped {inserted} value(s).")
            if skipped:
                st.warning(f"Skipped {len(skipped)} (errors).")
            st.rerun()
    with c2:
        if not available_df.empty and st.button("➕ Add ALL filtered", use_container_width=True):
            inserted, skipped = add_mappings(node_id, ids_avail)
            st.success(f"Mapped {inserted} value(s).")
            if skipped:
                st.warning(f"Skipped {len(skipped)} (errors).")
            st.rerun()

with right:
    st.subheader("Currently mapped values")
    search_mapped = st.text_input("Filter mapped values", key="search_mapped", placeholder="Type to filter…")
    mapped_df = fetch_mapped_values(node_id, search_mapped.strip() or None)

    if mapped_df.empty:
        st.info("No mapped values match the filter.")
        to_remove_ids = []
    else:
        st.dataframe(
            mapped_df.rename(columns={
                "pillarNodeValue": "Value",
                "pillarNodeValueDescription": "Description",
                "dateAdded": "Mapped on"
            }),
            use_container_width=True,
            hide_index=True
        )
        labels_map = [f"{r.pillarNodeValue} (id={int(r.id)})" for r in mapped_df.itertuples()]
        ids_map = [int(r.id) for r in mapped_df.itertuples()]
        choices_rm = list(zip(labels_map, ids_map))
        to_remove = st.multiselect(
            "Select values to REMOVE",
            options=choices_rm,
            format_func=lambda t: t[0],
            key="to_remove_multiselect",
        )
        to_remove_ids = [t[1] for t in to_remove]

    c3, c4 = st.columns([1,1])
    with c3:
        if st.button("🗑️ Remove selected", type="primary", use_container_width=True, disabled=(len(to_remove_ids) == 0)):
            removed = remove_mappings(node_id, to_remove_ids)
            st.success(f"Removed {removed} mapping(s).")
            st.rerun()
    with c4:
        if not mapped_df.empty and st.button("🗑️ Remove ALL filtered", use_container_width=True):
            removed = remove_mappings(node_id, ids_map)
            st.success(f"Removed {removed} mapping(s).")
            st.rerun()

st.markdown("---")
st.caption(f"{node_label} · Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
