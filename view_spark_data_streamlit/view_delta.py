import os, pandas as pd, streamlit as st
from deltalake import DeltaTable

st.set_page_config(page_title="CDC Viewer", layout="wide")
ROOT = "/Users/vinushan/Desktop/lakehouse/delta"

TABLES = {
    "Clean": os.path.join(ROOT, "customers_cdc_clean"),
    "Quarantine": os.path.join(ROOT, "customers_cdc_quarantine"),
    "Deletes": os.path.join(ROOT, "customers_cdc_deletes"),
}

@st.cache_data(ttl=5)
def load_delta(path: str, limit: int|None):
    dt = DeltaTable(path)
    df = dt.to_pandas()          
    return df if limit is None else df.head(limit)

st.title("CDC Tables (Delta)")
top = st.columns([2,1,1,2])
with top[0]:
    table_name = st.selectbox("Table", list(TABLES.keys()))
path = TABLES[table_name]
with top[1]:
    limit = st.number_input("Max rows (0 = all)", 0, 1_000_000, 1000, 100)
    limit = None if limit == 0 else int(limit)
with top[2]:
    if st.button("🔄 Refresh"):
        load_delta.clear()
with top[3]:
    st.caption(path)


flt_col, flt_val = st.columns(2)
with flt_col: colname = st.text_input("Filter column (optional)", "")
with flt_val: val = st.text_input("Contains…", "")

try:
    df = load_delta(path, limit)
    if colname and val and colname in df.columns:
        df = df[df[colname].astype(str).str.contains(val, case=False, na=False)]
    st.write(f"Rows showing: {len(df):,}")
    st.dataframe(df, use_container_width=True)
    st.download_button("Download CSV", df.to_csv(index=False), f"{table_name.lower()}.csv", "text/csv")
except Exception as e:
    st.error(f"Failed to read Delta: {e}")