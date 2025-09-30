# pages/06_WarningRules.py
import os
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

st.set_page_config(page_title="Warning Rules (Combinations)", layout="wide")

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
        parts.append("Trusted_Connection=yes")
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
# -----------------------------


# -----------------------------
# Taxonomy helpers
# -----------------------------
def fetch_categories() -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text("SELECT id, category FROM dbo.category ORDER BY category;"), cx)

def fetch_subcategories(category_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(
            text("SELECT id, subCategory FROM dbo.subCategory WHERE category_id = :cid ORDER BY subCategory;"),
            cx, params={"cid": category_id}
        )

def fetch_nodes(subcat_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(
            text("""SELECT id, pillarNode, pillarNodeDescription
                    FROM dbo.pillarNode WHERE subCategory_id = :sid ORDER BY pillarNode;"""),
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

# -----------------------------
# Rules CRUD
# -----------------------------
def list_rules() -> pd.DataFrame:
    sql = text("""
        SELECT r.id, r.name, r.severity, r.isActive, r.dataSourceFilter, r.dateAdded,
               STRING_AGG(
                   CONCAT(n.pillarNode, ' ', c.operator, ' ',
                          COALESCE(v.pillarNodeValue, 'NULL')
                   ), ' AND '
               ) WITHIN GROUP (ORDER BY c.id) AS conditions
        FROM dbo.warningRule r
        LEFT JOIN dbo.warningRuleCondition c ON c.rule_id = r.id
        LEFT JOIN dbo.pillarNode n ON n.id = c.pillarNode_id
        LEFT JOIN dbo.pillarNodeValue v ON v.id = c.pillarNodeValue_id
        GROUP BY r.id, r.name, r.severity, r.isActive, r.dataSourceFilter, r.dateAdded
        ORDER BY r.dateAdded DESC, r.id DESC;
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx)

def fetch_rule(rule_id: int) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text("SELECT * FROM dbo.warningRule WHERE id = :id;"), cx, params={"id": rule_id})

def fetch_rule_conditions(rule_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT c.id, c.rule_id, c.pillarNode_id, n.pillarNode, c.operator, c.pillarNodeValue_id, v.pillarNodeValue
        FROM dbo.warningRuleCondition c
        JOIN dbo.pillarNode n ON n.id = c.pillarNode_id
        LEFT JOIN dbo.pillarNodeValue v ON v.id = c.pillarNodeValue_id
        WHERE c.rule_id = :rid
        ORDER BY c.id;
    """)
    with engine.begin() as cx:
        return pd.read_sql(sql, cx, params={"rid": rule_id})

def insert_rule(name: str, message: str, severity: str, is_active: bool, ds_filter: Optional[str], conditions: List[dict]) -> int:
    with engine.begin() as cx:
        rid = cx.execute(
            text("""INSERT INTO dbo.warningRule (name, message, severity, isActive, dataSourceFilter)
                    OUTPUT INSERTED.id
                    VALUES (:n, :m, :s, :a, :d);"""),
            {"n": name, "m": message, "s": severity, "a": 1 if is_active else 0, "d": ds_filter or None}
        ).scalar_one()
        for cond in conditions:
            cx.execute(
                text("""INSERT INTO dbo.warningRuleCondition (rule_id, pillarNode_id, operator, pillarNodeValue_id)
                        VALUES (:rid, :nid, :op, :vid);"""),
                {"rid": rid, "nid": int(cond["node_id"]), "op": cond["operator"], "vid": cond.get("value_id", None)}
            )
    return int(rid)

def update_rule_meta(rule_id: int, name: str, message: str, severity: str, is_active: bool, ds_filter: Optional[str]):
    with engine.begin() as cx:
        cx.execute(
            text("""UPDATE dbo.warningRule
                    SET name=:n, message=:m, severity=:s, isActive=:a, dataSourceFilter=:d
                    WHERE id=:id;"""),
            {"id": rule_id, "n": name, "m": message, "s": severity, "a": 1 if is_active else 0, "d": ds_filter or None}
        )

def delete_rule(rule_id: int):
    with engine.begin() as cx:
        cx.execute(text("DELETE FROM dbo.warningRule WHERE id = :id;"), {"id": rule_id})

def add_condition(rule_id: int, node_id: int, operator: str, value_id: Optional[int]):
    with engine.begin() as cx:
        cx.execute(
            text("""INSERT INTO dbo.warningRuleCondition (rule_id, pillarNode_id, operator, pillarNodeValue_id)
                    VALUES (:rid, :nid, :op, :vid);"""),
            {"rid": rule_id, "nid": node_id, "op": operator, "vid": value_id}
        )

def delete_condition(cond_id: int):
    with engine.begin() as cx:
        cx.execute(text("DELETE FROM dbo.warningRuleCondition WHERE id = :id;"), {"id": cond_id})

# -----------------------------
# Optional: evaluate rules for a user+source
# -----------------------------
def fetch_user_pref_map(user_name: str, data_source: str) -> Dict[int, Optional[int]]:
    with engine.begin() as cx:
        rows = cx.execute(
            text("""SELECT pillarNode_id, pillarNodeValue_id
                    FROM dbo.userNodePreference
                    WHERE userName=:u AND dataSource=:ds;"""),
            {"u": user_name, "ds": data_source}
        ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}

def evaluate_rules(user_name: str, data_source: str) -> pd.DataFrame:
    prefs = fetch_user_pref_map(user_name, data_source)
    rules = list_rules()
    hits = []
    with engine.begin() as cx:
        for r in rules.itertuples():
            if not r.isActive:
                continue
            if r.dataSourceFilter and r.dataSourceFilter != data_source:
                continue
            conds = fetch_rule_conditions(int(r.id))
            ok = True
            for c in conds.itertuples():
                val = prefs.get(int(c.pillarNode_id))  # may be None
                op = c.operator.upper()
                target = int(c.pillarNodeValue_id) if pd.notna(c.pillarNodeValue_id) else None
                if op == '=':
                    ok &= (val == target)
                elif op in ('!=', '<>'):
                    ok &= (val is None or val != target)
                elif op == 'IS NULL':
                    ok &= (val is None)
                elif op == 'IS NOT NULL':
                    ok &= (val is not None)
                else:
                    ok = False
                if not ok:
                    break
            if ok:
                hits.append({"id": r.id, "name": r.name, "severity": r.severity, "message": r.message})
    return pd.DataFrame(hits)

# -----------------------------
# UI
# -----------------------------
st.title("⚠️ Warning Rules — Define Combinations")
st.caption("Create rules like: **Source Format = Unstructured AND Target Platform = Structured → show warning**.")

OPERATORS = ["=", "!=", "IS NULL", "IS NOT NULL"]
SEVERITIES = ["Info", "Warning", "Error"]

# --- Sidebar: quick preview evaluator (optional) ---
with st.sidebar:
    st.header("Preview warnings (optional)")
    default_user = os.getenv("USERNAME") or os.getenv("USER") or "me"
    prev_user = st.text_input("User", value=default_user)
    prev_source = st.text_input("Data Source", placeholder="e.g., Salesforce API")
    if st.button("Run preview"):
        if not prev_source.strip():
            st.warning("Enter a Data Source name to preview.")
        else:
            df_hits = evaluate_rules(prev_user.strip(), prev_source.strip())
            if df_hits.empty:
                st.success("No rules triggered.")
            else:
                st.write(df_hits)

st.markdown("---")

# ========== Create New Rule ==========
st.subheader("➕ Create a new rule")
with st.form("new_rule_meta_form"):
    c1, c2, c3 = st.columns([2,1,1])
    name = c1.text_input("Rule name", placeholder="e.g., Unstructured source into Structured target")
    severity = c2.selectbox("Severity", SEVERITIES, index=1)
    is_active = c3.checkbox("Active", value=True)
    message = st.text_area("Message (shown to user)", max_chars=400,
                           placeholder="Add a staging/parsing step (OCR/extraction/schema mapping) or land to a lake first.")
    ds_filter = st.text_input("Limit to Data Source (optional)", placeholder="Leave blank to apply to all sources")
    submitted_meta = st.form_submit_button("Start rule & add conditions")
    if submitted_meta:
        if not name.strip() or not message.strip():
            st.error("Name and Message are required.")
        else:
            st.session_state["new_rule_meta"] = {
                "name": name.strip(),
                "severity": severity,
                "is_active": is_active,
                "message": message.strip(),
                "ds_filter": ds_filter.strip() or None,
            }
            st.success("Rule draft created. Add conditions below.")

# Condition builder for the *new* rule (collect in session, then save)
if "new_rule_meta" in st.session_state:
    st.markdown("**Conditions (ANDed):**")
    if "new_rule_conds" not in st.session_state:
        st.session_state["new_rule_conds"] = []

    # Picker
    cat_df = fetch_categories()
    if cat_df.empty:
        st.info("No categories found. Add categories first.")
    else:
        cA, cB, cC, cD = st.columns([1,1,1,1])
        cat_choice = cA.selectbox("Category", options=[(int(r.id), r.category) for r in cat_df.itertuples()], format_func=lambda t: t[1])
        sub_df = fetch_subcategories(cat_choice[0])
        sub_choice = cB.selectbox("Subcategory", options=[(int(r.id), r.subCategory) for r in sub_df.itertuples()], format_func=lambda t: t[1]) if not sub_df.empty else None
        node_df = fetch_nodes(sub_choice[0]) if sub_choice else pd.DataFrame()
        node_choice = cC.selectbox("Pillar Node", options=[(int(r.id), r.pillarNode) for r in node_df.itertuples()], format_func=lambda t: t[1]) if not node_df.empty else None
        op_choice = cD.selectbox("Operator", OPERATORS)

        val_choice = None
        if node_choice:
            vals_df = fetch_values_for_node(node_choice[0])
            if op_choice in ("=", "!="):
                if vals_df.empty:
                    st.info("This node has no mapped values.")
                else:
                    val_choice = st.selectbox("Value", options=[(int(r.id), r.pillarNodeValue) for r in vals_df.itertuples()], format_func=lambda t: t[1])

        cE, cF = st.columns([1,1])
        if cE.button("Add condition"):
            if not node_choice:
                st.error("Choose a Pillar Node.")
            elif op_choice in ("=", "!=") and not val_choice:
                st.error("Select a Value for '=' or '!='.")
            else:
                st.session_state["new_rule_conds"].append({
                    "node_id": node_choice[0],
                    "node_label": node_choice[1],
                    "operator": op_choice,
                    "value_id": (val_choice[0] if val_choice else None),
                    "value_label": (val_choice[1] if val_choice else "NULL"),
                })

        if st.session_state["new_rule_conds"]:
            st.table(pd.DataFrame([{
                "Node": c["node_label"],
                "Operator": c["operator"],
                "Value": c["value_label"]
            } for c in st.session_state["new_rule_conds"]]))
            cG, cH = st.columns([1,1])
            if cG.button("🔄 Clear conditions"):
                st.session_state["new_rule_conds"] = []
            if cH.button("💾 Save rule"):
                rid = insert_rule(
                    st.session_state["new_rule_meta"]["name"],
                    st.session_state["new_rule_meta"]["message"],
                    st.session_state["new_rule_meta"]["severity"],
                    st.session_state["new_rule_meta"]["is_active"],
                    st.session_state["new_rule_meta"]["ds_filter"],
                    st.session_state["new_rule_conds"]
                )
                st.success(f"Rule saved (id={rid}).")
                del st.session_state["new_rule_meta"]
                del st.session_state["new_rule_conds"]
                st.rerun()

st.markdown("---")

# ========== Existing Rules ==========
st.subheader("📚 Existing rules")
rules_df = list_rules()
if rules_df.empty:
    st.info("No rules yet.")
else:
    st.dataframe(
        rules_df.rename(columns={
            "dataSourceFilter":"Scope (dataSource)",
            "conditions":"Conditions"
        }),
        use_container_width=True, hide_index=True
    )

    # Select a rule to edit
    pick = st.selectbox("Select a rule to edit", options=[(int(r.id), f"[{r.severity}{'•off' if not r.isActive else ''}] {r.name} (id={int(r.id)})") for r in rules_df.itertuples()], format_func=lambda t: t[1])
    rid = pick[0]

    meta = fetch_rule(rid).iloc[0]
    with st.expander("✏️ Edit rule meta", expanded=False):
        e1, e2, e3 = st.columns([2,1,1])
        new_name = e1.text_input("Name", value=meta["name"])
        new_sev  = e2.selectbox("Severity", SEVERITIES, index=SEVERITIES.index(meta["severity"]) if meta["severity"] in SEVERITIES else 1)
        new_act  = e3.checkbox("Active", value=bool(meta["isActive"]))
        new_msg  = st.text_area("Message", value=meta["message"], max_chars=400)
        new_ds   = st.text_input("Data Source scope (optional)", value=meta["dataSourceFilter"] or "")
        c1, c2, c3 = st.columns([1,1,1])
        if c1.button("💾 Save meta"):
            update_rule_meta(rid, new_name.strip(), new_msg.strip(), new_sev, new_act, new_ds.strip() or None)
            st.success("Saved.")
            st.rerun()
        if c2.button("🗑️ Delete rule"):
            delete_rule(rid)
            st.success("Deleted.")
            st.rerun()
        if c3.button("⏯️ Toggle active"):
            update_rule_meta(rid, meta["name"], meta["message"], meta["severity"], not bool(meta["isActive"]), meta["dataSourceFilter"])
            st.rerun()

    with st.expander("🧩 Conditions", expanded=True):
        conds = fetch_rule_conditions(rid)
        if conds.empty:
            st.info("No conditions yet.")
        else:
            st.dataframe(
                conds.rename(columns={
                    "pillarNode":"Node",
                    "operator":"Operator",
                    "pillarNodeValue":"Value"
                }),
                use_container_width=True, hide_index=True
            )
            # Delete a condition
            del_sel = st.selectbox(
                "Delete a condition",
                options=[(int(r.id), f"{r.pillarNode} {r.operator} {r.pillarNodeValue or 'NULL'} (id={int(r.id)})") for r in conds.itertuples()],
                format_func=lambda t: t[1],
                key="del_cond_sel"
            )
            if st.button("🗑️ Delete selected condition"):
                delete_condition(del_sel[0])
                st.success("Condition deleted.")
                st.rerun()

        st.markdown("**Add a condition**")
        cA, cB, cC, cD = st.columns([1,1,1,1])
        cat_df = fetch_categories()
        if cat_df.empty:
            st.info("No categories.")
        else:
            cat_choice = cA.selectbox("Category", options=[(int(r.id), r.category) for r in cat_df.itertuples()], format_func=lambda t: t[1], key="e_cat")
            sub_df = fetch_subcategories(cat_choice[0])
            sub_choice = cB.selectbox("Subcategory", options=[(int(r.id), r.subCategory) for r in sub_df.itertuples()], format_func=lambda t: t[1], key="e_sub") if not sub_df.empty else None
            node_df = fetch_nodes(sub_choice[0]) if sub_choice else pd.DataFrame()
            node_choice = cC.selectbox("Node", options=[(int(r.id), r.pillarNode) for r in node_df.itertuples()], format_func=lambda t: t[1], key="e_node") if not node_df.empty else None
            op_choice = cD.selectbox("Operator", OPERATORS, key="e_op")

            val_choice = None
            if node_choice and op_choice in ("=", "!="):
                vals_df = fetch_values_for_node(node_choice[0])
                if vals_df.empty:
                    st.info("This node has no mapped values.")
                else:
                    val_choice = st.selectbox("Value", options=[(int(r.id), r.pillarNodeValue) for r in vals_df.itertuples()], format_func=lambda t: t[1], key="e_val")

            if st.button("➕ Add condition to rule"):
                if not node_choice:
                    st.error("Pick a node.")
                elif op_choice in ("=", "!=") and not val_choice:
                    st.error("Pick a value for '=' or '!='.")
                else:
                    add_condition(rid, node_choice[0], op_choice, (val_choice[0] if val_choice else None))
                    st.success("Condition added.")
                    st.rerun()

st.markdown("---")
st.caption(f"Connected to **{st.secrets['sqlserver']['database']}** · {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
