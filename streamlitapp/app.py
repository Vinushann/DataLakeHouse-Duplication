import streamlit as st, pandas as pd, mysql.connector
from mysql.connector import Error

st.set_page_config(page_title="Customers Admin", layout="centered")

# ---------- DB ----------
@st.cache_resource
def conn():
    cfg = st.secrets["db"]
    return mysql.connector.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"],
        password=cfg["password"], database=cfg["database"], autocommit=False
    )

def ensure_table():
    sql = """CREATE TABLE IF NOT EXISTS customers(
      id INT PRIMARY KEY,
      name VARCHAR(100),
      email VARCHAR(200),
      updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );"""
    c = conn().cursor(); c.execute(sql); conn().commit(); c.close()

# ---------- Data funcs ----------
@st.cache_data(ttl=10, show_spinner=False)
def fetch_all():
    c = conn().cursor(dictionary=True)
    c.execute("SELECT id,name,email,updated_at FROM customers ORDER BY id")
    rows = c.fetchall(); c.close()
    return pd.DataFrame(rows)

def update_customer(cid, name, email):
    c = conn().cursor()
    c.execute("UPDATE customers SET name=%s, email=%s WHERE id=%s",
              (name, email, int(cid)))
    conn().commit(); c.close()

def insert_customer(cid, name, email):
    c = conn().cursor()
    c.execute("INSERT INTO customers (id,name,email) VALUES (%s,%s,%s)",
              (int(cid), name, email))
    conn().commit(); c.close()

def next_id():
    c = conn().cursor()
    c.execute("SELECT COALESCE(MAX(id),0)+1 FROM customers")
    n = c.fetchone()[0]; c.close(); return n

def delete_customer(cid: int):
    c = conn().cursor()
    try:
        c.execute("DELETE FROM customers WHERE id=%s", (int(cid),))
        conn().commit()
        return c.rowcount
    finally:
        c.close()

# ---------- UI ----------
ensure_table()
st.title("Customers (cog)")

top = st.columns([1,1,3])
with top[0]:
    if st.button("🔄 Refresh", use_container_width=True):
        fetch_all.clear()
with top[1]:
    with st.popover("➕ Insert"):
        try:
            suggested = int(next_id())
            cid = st.number_input("ID", min_value=1, value=suggested, step=1, key="ins_id")
            name = st.text_input("Name", key="ins_name")
            email = st.text_input("Email", key="ins_email")
            if st.button("Create", key="ins_btn"):
                if not name.strip() or not email.strip():
                    st.warning("Name and Email are required.")
                else:
                    try:
                        insert_customer(cid, name.strip(), email.strip())
                        st.success("Inserted.")
                        fetch_all.clear()
                    except Error as e:
                        st.error(f"Insert failed: {e}")
        except Error as e:
            st.error(e)

tab1, tab2 = st.tabs(["Browse", "Update"])

with tab1:
    try:
        df = fetch_all()
        if df.empty:
            st.info("No customers yet.")
        else:
            st.dataframe(df, use_container_width=True)
            del_id = st.number_input("Delete by ID", min_value=1, step=1, value=1, key="del_direct_id")
            if st.button("Delete ID", key="del_direct_btn"):
                try:
                    n = delete_customer(del_id)
                    st.success(f"Deleted {n} row(s) for ID {del_id}.")
                    fetch_all.clear()
                except Error as e:
                    st.error(f"Delete failed: {e}")
    except Error as e:
        st.error(e)

with tab2:
    try:
        df = fetch_all()
        if df.empty:
            st.info("No customers to edit."); st.stop()
        cid = st.selectbox("Pick customer ID", df["id"].tolist(), key="edit_pick")
        row = df[df.id == cid].iloc[0]
        name = st.text_input("Name", row["name"], key="edit_name")
        email = st.text_input("Email", row["email"], key="edit_email")
        if st.button("Save changes", key="edit_save"):
            try:
                update_customer(cid, name.strip(), email.strip())
                st.success("Updated.")
                fetch_all.clear()
            except Error as e:
                st.error(f"Update failed: {e}")

        st.divider()
        colA, colB = st.columns([1,1])
        with colA:
            if st.button("🗑️ Delete customer", key="edit_delete"):
                st.session_state["_pending_delete"] = int(cid)
        with colB:
            if st.session_state.get("_pending_delete") == int(cid):
                if st.checkbox("Type-safe confirm (irreversible)", key="edit_confirm"):
                    try:
                        n = delete_customer(cid)
                        if n == 0:
                            st.warning(f"No row with ID {cid}.")
                        else:
                            st.success(f"Deleted ID {cid}.")
                        st.session_state.pop("_pending_delete", None)
                        fetch_all.clear()
                    except Error as e:
                        st.error(f"Delete failed: {e}")
    except Error as e:
        st.error(e)

with st.expander("Debug"):
    st.code(st.secrets["db"], language="toml")