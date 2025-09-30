# pages/03_PillarNodeValues.py
import urllib.parse
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

st.set_page_config(page_title="Pillar Node Values Admin", layout="wide")

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
def fetch_values(search: str | None = None) -> pd.DataFrame:
    base = """
        SELECT v.id,
               v.pillarNodeValue,
               v.pillarNodeValueDescription,
               v.dateAdded,
               COUNT(m.pillarNode_id) AS mappingCount
        FROM dbo.pillarNodeValue v
        LEFT JOIN dbo.pillarNodeValueMapping m
          ON m.pillarNodeValue_id = v.id
    """
    where = " WHERE v.pillarNodeValue LIKE :pat OR v.pillarNodeValueDescription LIKE :pat " if search else ""
    tail = " GROUP BY v.id, v.pillarNodeValue, v.pillarNodeValueDescription, v.dateAdded ORDER BY v.pillarNodeValue;"
    sql = text(base + where + tail)
    params = {"pat": f"%{search}%"} if search else {}
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params=params)

def value_exists(name: str) -> bool:
    sql = text("""
        SELECT 1
        FROM dbo.pillarNodeValue
        WHERE LTRIM(RTRIM(pillarNodeValue)) = LTRIM(RTRIM(:name));
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"name": name}).fetchone()
        return row is not None

def insert_value(name: str, desc: str | None):
    sql = text("""
        INSERT INTO dbo.pillarNodeValue (pillarNodeValue, pillarNodeValueDescription)
        VALUES (:name, :desc);
    """)
    with engine.begin() as cx:
        cx.execute(sql, {"name": name, "desc": desc if desc else None})

def update_value(val_id: int, name: str, desc: str | None):
    sql = text("""
        UPDATE dbo.pillarNodeValue
        SET pillarNodeValue = :name,
            pillarNodeValueDescription = :desc
        WHERE id = :id;
    """)
    with engine.begin() as cx:
        cx.execute(sql, {"id": val_id, "name": name, "desc": desc if desc else None})

def mapping_count_for_value(val_id: int) -> int:
    sql = text("""
        SELECT COUNT(*) AS cnt
        FROM dbo.pillarNodeValueMapping
        WHERE pillarNodeValue_id = :vid;
    """)
    with engine.begin() as cx:
        row = cx.execute(sql, {"vid": val_id}).fetchone()
        return int(row[0]) if row else 0

def delete_value(val_id: int) -> tuple[bool, str]:
    cnt = mapping_count_for_value(val_id)
    if cnt > 0:
        return False, f"Cannot delete: {cnt} mapping(s) exist in pillarNodeValueMapping. Remove those first."
    try:
        with engine.begin() as cx:
            cx.execute(text("DELETE FROM dbo.pillarNodeValue WHERE id = :id;"), {"id": val_id})
        return True, f"Deleted pillar node value id={val_id}."
    except DBAPIError as e:
        return False, f"Delete failed: {e.orig if hasattr(e, 'orig') else e}"

# -----------------------------
# UI
# -----------------------------
st.title("🔗 Pillar Node Values Admin (SQL Server)")
st.caption("Manage **dbo.pillarNodeValue** entries (add, edit, delete). Deletions are blocked if mapped to any pillar nodes.")

# Controls row
search = st.text_input("Search values/description", placeholder="Type to filter…")
values_df = fetch_values(search.strip() or None)

left, right = st.columns([1, 2], gap="large")

# ---- Add
with left:
    st.subheader("➕ Add a value")
    with st.form("add_value_form", clear_on_submit=True):
        name = st.text_input("Value (varchar(50))", max_chars=50, placeholder="e.g., RBAC, CDC, Purge policy")
        desc = st.text_area("Description (varchar(200), optional)", max_chars=200, height=90, placeholder="Short description…")
        submitted = st.form_submit_button("Add value")
        if submitted:
            n = (name or "").strip()
            d = (desc or "").strip()
            if not n:
                st.error("Value is required.")
            elif value_exists(n):
                st.warning(f"'{n}' already exists.")
            else:
                insert_value(n, d if d else None)
                st.success(f"Added '{n}'.")
                st.rerun()

# ---- Existing table + Edit/Delete
with right:
    st.subheader("Existing values")
    if values_df.empty:
        st.info("No values found.")
    else:
        st.dataframe(
            values_df.rename(columns={
                "pillarNodeValue": "Value",
                "pillarNodeValueDescription": "Description",
                "dateAdded": "Added",
                "mappingCount": "Mappings"
            }),
            use_container_width=True,
            hide_index=True
        )

        with st.expander("✏️ Update a value", expanded=False):
            labels = [f"{r.pillarNodeValue} (id={int(r.id)}, mappings={int(r.mappingCount)})" for r in values_df.itertuples()]
            ids = [int(r.id) for r in values_df.itertuples()]
            sel = st.selectbox("Choose value", options=list(zip(labels, ids)), format_func=lambda t: t[0])
            if sel:
                _, val_id = sel
                current = values_df[values_df["id"] == val_id].iloc[0]
                new_name = st.text_input("New value", value=current["pillarNodeValue"], max_chars=50)
                new_desc = st.text_area("New description", value=current["pillarNodeValueDescription"] or "", max_chars=200, height=90)
                if st.button("Save changes", type="primary"):
                    nn = (new_name or "").strip()
                    nd = (new_desc or "").strip()
                    if not nn:
                        st.error("Value cannot be empty.")
                    else:
                        # prevent global dup if changed
                        if nn.lower().strip() != str(current["pillarNodeValue"]).lower().strip() and value_exists(nn):
                            st.warning(f"'{nn}' already exists.")
                        else:
                            update_value(val_id, nn, nd if nd else None)
                            st.success("Updated.")
                            st.rerun()

        with st.expander("🗑️ Delete a value", expanded=False):
            del_sel = st.selectbox(
                "Choose value to delete",
                options=list(zip(labels, ids)),
                format_func=lambda t: t[0],
                key="delete_select_value",
            )
            if st.button("Delete selected", type="primary"):
                _, del_id = del_sel
                ok, msg = delete_value(int(del_id))
                (st.success if ok else st.error)(msg)
                if ok:
                    st.rerun()

st.markdown("---")
st.caption(f"Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
