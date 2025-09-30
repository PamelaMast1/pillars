import urllib.parse
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Subcategories Admin", layout="wide")

# -----------------------------
# Connection helpers (SQL Server)
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
        parts.append("Trusted_Connection=yes")   # or: Integrated Security=SSPI
    else:
        parts += [f"UID={cfg['username']}", f"PWD={cfg['password']}"]

    odbc = ";".join(parts) + ";"
    return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc)

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(_build_sqlalchemy_url(), pool_pre_ping=True, fast_executemany=True)

engine = get_engine()

# -----------------------------
# Data access
# -----------------------------
def fetch_categories():
    sql = text("""
        SELECT id, category, dateAdded
        FROM dbo.category
        ORDER BY category
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx)

def fetch_subcategories(category_id: int):
    sql = text("""
        SELECT id, subCategory, dateAdded
        FROM dbo.subCategory
        WHERE category_id = :cid
        ORDER BY subCategory
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params={"cid": category_id})

def category_exists(name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM dbo.category
        WHERE LTRIM(RTRIM(category)) = LTRIM(RTRIM(:name))
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"name": name}).fetchone()
        return row is not None

def insert_category(name: str):
    sql = text("INSERT INTO dbo.category (category) VALUES (:name)")
    with engine.begin() as cx:
        cx.execute(sql, {"name": name})

def subcategory_exists(category_id: int, name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM dbo.subCategory
        WHERE category_id = :cid
          AND LTRIM(RTRIM(subCategory)) = LTRIM(RTRIM(:name))
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"cid": category_id, "name": name}).fetchone()
        return row is not None

def insert_subcategory(category_id: int, name: str):
    sql = text("""
        INSERT INTO dbo.subCategory (category_id, subCategory)
        VALUES (:cid, :name)
    """)
    with engine.begin() as cx:
        cx.execute(sql, {"cid": category_id, "name": name})

def delete_subcategory(subcat_id: int):
    sql = text("DELETE FROM dbo.subCategory WHERE id = :id")
    with engine.begin() as cx:
        cx.execute(sql, {"id": subcat_id})

# -----------------------------
# UI
# -----------------------------
st.title("🧱 Subcategories Admin (SQL Server)")
st.caption("Add new **subCategory** rows keyed to existing **category** values in the `pillars` database.")

with st.sidebar:
    st.header("Quick actions")
    st.write("Use this to create a missing **Category** first.")
    with st.form("add_category_form", clear_on_submit=True):
        new_cat = st.text_input("New category name (varchar(30))", max_chars=30)
        submitted_cat = st.form_submit_button("➕ Add category")
        if submitted_cat:
            name = (new_cat or "").strip()
            if not name:
                st.error("Category name is required.")
            else:
                if category_exists(name):
                    st.warning(f"Category '{name}' already exists.")
                else:
                    insert_category(name)
                    st.success(f"Category '{name}' added.")
                    st.rerun()

# Main columns
left, right = st.columns([1, 2], gap="large")

with left:
    st.subheader("1) Choose a Category")
    cat_df = fetch_categories()

    if cat_df.empty:
        st.info("No categories yet. Add one in the sidebar.")
        st.stop()

    # Map for select
    cat_options = {f"{row.category} (id={row.id})": int(row.id) for row in cat_df.itertuples()}
    selected_label = st.selectbox("Category", list(cat_options.keys()))
    selected_category_id = cat_options[selected_label]

    st.markdown("---")
    st.subheader("2) Add a Subcategory")
    with st.form("add_subcat_form", clear_on_submit=True):
        new_sub = st.text_input("Subcategory name (varchar(50))", max_chars=50, placeholder="e.g., RBAC, Purge Jobs, CDC Connectors")
        submitted_sub = st.form_submit_button("➕ Add subcategory")
        if submitted_sub:
            subname = (new_sub or "").strip()
            if not subname:
                st.error("Subcategory name is required.")
            else:
                if subcategory_exists(selected_category_id, subname):
                    st.warning(f"'{subname}' already exists for this category.")
                else:
                    insert_subcategory(selected_category_id, subname)
                    st.success(f"Subcategory '{subname}' added to {selected_label}.")
                    st.rerun()

with right:
    st.subheader("Existing Subcategories")
    existing = fetch_subcategories(selected_category_id)
    if existing.empty:
        st.info("No subcategories yet for this category.")
    else:
        st.dataframe(existing, use_container_width=True, hide_index=True)

        with st.expander("🗑️ Delete a subcategory", expanded=False):
            # Simple delete control (kept explicit to avoid accidental edits)
            ids = [int(r.id) for r in existing.itertuples()]
            labels = [f"{r.subCategory} (id={int(r.id)})" for r in existing.itertuples()]
            if ids:
                del_choice = st.selectbox("Choose subcategory to delete", options=list(zip(labels, ids)), format_func=lambda t: t[0])
                if st.button("Delete selected", type="primary"):
                    _, del_id = del_choice
                    delete_subcategory(int(del_id))
                    st.success(f"Deleted subcategory id={del_id}.")
                    st.rerun()

# Footer
st.markdown("---")
st.caption(f"Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

