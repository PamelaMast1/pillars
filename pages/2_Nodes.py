# pages/02_PillarNodes.py
import urllib.parse
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

st.set_page_config(page_title="Pillar Nodes Admin", layout="wide")

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
# Data access helpers
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

def pillar_node_exists(subcat_id: int, name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM dbo.pillarNode
        WHERE subCategory_id = :sid
          AND LTRIM(RTRIM(pillarNode)) = LTRIM(RTRIM(:name));
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"sid": subcat_id, "name": name}).fetchone()
        return row is not None

def insert_pillar_node(subcat_id: int, name: str, desc: str | None):
    sql = text("""
        INSERT INTO dbo.pillarNode (subCategory_id, pillarNode, pillarNodeDescription)
        VALUES (:sid, :name, :desc);
    """)
    with engine.begin() as cx:
        cx.execute(sql, {"sid": subcat_id, "name": name, "desc": desc if desc else None})

def update_pillar_node(node_id: int, name: str, desc: str | None):
    sql = text("""
        UPDATE dbo.pillarNode
        SET pillarNode = :name,
            pillarNodeDescription = :desc
        WHERE id = :id;
    """)
    with engine.begin() as cx:
        cx.execute(sql, {"id": node_id, "name": name, "desc": desc if desc else None})

def mapping_count_for_node(node_id: int) -> int:
    sql = text("""
        SELECT COUNT(*) AS cnt
        FROM dbo.pillarNodeValueMapping
        WHERE pillarNode_id = :pid;
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"pid": node_id}).fetchone()
        return int(row[0]) if row else 0

def delete_pillar_node(node_id: int) -> tuple[bool, str]:
    # Guard against FK violations to mapping table
    cnt = mapping_count_for_node(node_id)
    if cnt > 0:
        return False, f"Cannot delete: {cnt} mapping(s) exist in pillarNodeValueMapping. Remove those first."
    try:
        with engine.begin() as cx:
            cx.execute(text("DELETE FROM dbo.pillarNode WHERE id = :id;"), {"id": node_id})
        return True, f"Deleted pillar node id={node_id}."
    except DBAPIError as e:
        return False, f"Delete failed: {e.orig if hasattr(e, 'orig') else e}"

# -----------------------------
# UI
# -----------------------------
st.title("🌿 Pillar Nodes Admin (SQL Server)")
st.caption("Manage **dbo.pillarNode** entries by Category → Subcategory.")

left, right = st.columns([1, 2], gap="large")

with left:
    st.subheader("1) Choose Category")
    cat_df = fetch_categories()
    if cat_df.empty:
        st.info("No categories found. Create some on the Subcategories page.")
        st.stop()
    cat_options = {f"{r.category} (id={int(r.id)})": int(r.id) for r in cat_df.itertuples()}
    cat_label = st.selectbox("Category", list(cat_options.keys()))
    cat_id = cat_options[cat_label]

    st.subheader("2) Choose Subcategory")
    sub_df = fetch_subcategories(cat_id)
    if sub_df.empty:
        st.info("No subcategories under this category yet.")
        st.stop()
    sub_options = {f"{r.subCategory} (id={int(r.id)})": int(r.id) for r in sub_df.itertuples()}
    sub_label = st.selectbox("Subcategory", list(sub_options.keys()))
    sub_id = sub_options[sub_label]

    st.markdown("---")
    st.subheader("3) Add a Pillar Node")
    with st.form("add_node_form", clear_on_submit=True):
        name = st.text_input("Pillar node (varchar(50))", max_chars=50, placeholder="e.g., RBAC, CDC Connectors, Purge Jobs")
        desc = st.text_area("Description (varchar(200), optional)", max_chars=200, height=90, placeholder="Short description…")
        submitted = st.form_submit_button("➕ Add pillar node")
        if submitted:
            n = (name or "").strip()
            d = (desc or "").strip()
            if not n:
                st.error("Name is required.")
            elif pillar_node_exists(sub_id, n):
                st.warning(f"'{n}' already exists in {sub_label}.")
            else:
                insert_pillar_node(sub_id, n, d if d else None)
                st.success(f"Added '{n}' to {sub_label}.")
                st.rerun()

with right:
    st.subheader("Existing Pillar Nodes")
    nodes = fetch_pillar_nodes(sub_id)
    if nodes.empty:
        st.info("No pillar nodes yet for this subcategory.")
    else:
        st.dataframe(nodes, use_container_width=True, hide_index=True)

        with st.expander("✏️ Update a pillar node", expanded=False):
            labels = [f"{r.pillarNode} (id={int(r.id)})" for r in nodes.itertuples()]
            ids = [int(r.id) for r in nodes.itertuples()]
            sel = st.selectbox("Choose node", options=list(zip(labels, ids)), format_func=lambda t: t[0])
            if sel:
                _, node_id = sel
                current = nodes[nodes["id"] == node_id].iloc[0]
                new_name = st.text_input("New name", value=current["pillarNode"], max_chars=50)
                new_desc = st.text_area("New description", value=current["pillarNodeDescription"] or "", max_chars=200, height=90)
                if st.button("Save changes", type="primary"):
                    nn = (new_name or "").strip()
                    nd = (new_desc or "").strip()
                    if not nn:
                        st.error("Name cannot be empty.")
                    else:
                        # prevent dup within same subcategory if name changed
                        if nn.lower().strip() != str(current["pillarNode"]).lower().strip() and pillar_node_exists(sub_id, nn):
                            st.warning(f"'{nn}' already exists in this subcategory.")
                        else:
                            update_pillar_node(node_id, nn, nd if nd else None)
                            st.success("Updated.")
                            st.rerun()

        with st.expander("🗑️ Delete a pillar node", expanded=False):
            del_sel = st.selectbox(
                "Choose node to delete",
                options=list(zip(labels, ids)),
                format_func=lambda t: t[0],
                key="delete_select",
            )
            if st.button("Delete selected", type="primary"):
                _, del_id = del_sel
                ok, msg = delete_pillar_node(int(del_id))
                (st.success if ok else st.error)(msg)
                if ok:
                    st.rerun()

st.markdown("---")
st.caption(f"Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
