import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine

st.set_page_config(page_title="IMDB Overview", layout="wide")
st.title("Overview of `fact_movies` Table")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}

user = DB_CONFIG['user']
password = DB_CONFIG['password']
host = DB_CONFIG['host']
database = DB_CONFIG['database']
conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"


@st.cache_data
def load_data():
    engine = create_engine(conn_str)
    query = "SELECT * FROM fact_movies LIMIT 10000"
    df = pd.read_sql(query, engine)

    for col in ['startYear', 'endYear', 'runtimeMinutes', 'averageRating', 'numVotes']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

df = load_data()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📏 Dimensions",
    "🧬 Column Types",
    "🚫 Missing Values",
    "📄 Sample Data",
    "📊 Descriptive Stats"
])

with tab1:
    st.subheader("📏 Dataset Dimensions")
    st.write(f"- Rows: **{df.shape[0]}**")
    st.write(f"- Columns: **{df.shape[1]}**")

with tab2:
    st.subheader("🧬 Data Types per Column")
    dtype_df = pd.DataFrame(df.dtypes, columns=["Data Type"])
    st.dataframe(dtype_df)

with tab3:
    st.subheader("🚫 Missing Value Percentage")
    missing_df = df.isnull().mean().round(4) * 100
    st.dataframe(missing_df.reset_index().rename(columns={"index": "Column", 0: "Missing (%)"}))

with tab4:
    st.subheader("📄 Sample Data (First 5 Rows)")
    st.dataframe(df.head())

with tab5:
    st.subheader("📊 Descriptive Statistics (Numerical Columns)")
    st.dataframe(df.describe())
