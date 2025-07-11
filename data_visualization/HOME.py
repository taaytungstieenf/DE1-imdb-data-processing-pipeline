import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Set up Streamlit UI
st.set_page_config(page_title="IMDB Data Visualization", layout="wide")
st.title("🎬 IMDB Movie Data Dashboard")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}

# SQLAlchemy connection
user = DB_CONFIG['user']
password = DB_CONFIG['password']
host = DB_CONFIG['host']
database = DB_CONFIG['database']
conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"

@st.cache_data
def load_data():
    engine = create_engine(conn_str)
    query = "SELECT * FROM fact_movies LIMIT 10000"
    return pd.read_sql(query, engine)

df = load_data()

st.subheader("📊 Raw Data Sample")
st.dataframe(df.head(20))

# Example Visualization
if 'startYear' in df.columns:
    st.subheader("🎞️ Number of Movies by Release Year")
    year_counts = df['startYear'].value_counts().sort_index()
    st.bar_chart(year_counts)

if 'genres' in df.columns:
    st.subheader("🍿 Top 10 Genres")
    genre_counts = df['genres'].value_counts().nlargest(10)
    fig, ax = plt.subplots()
    sns.barplot(x=genre_counts.values, y=genre_counts.index, ax=ax)
    st.pyplot(fig)

if 'averageRating' in df.columns:
    st.subheader("⭐ Distribution of Ratings")
    fig, ax = plt.subplots()
    sns.histplot(df['averageRating'].dropna(), bins=20, kde=True, ax=ax)
    st.pyplot(fig)
