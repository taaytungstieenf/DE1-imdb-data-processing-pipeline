import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from wordcloud import WordCloud
import os

st.set_page_config(page_title="Word Cloud", layout="centered")
st.title("️📊 Word Cloud of Primary Names")

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
    query = "SELECT primaryName FROM fact_movies WHERE primaryName IS NOT NULL LIMIT 10000"
    df = pd.read_sql(query, engine)
    return df

df = load_data()

names = ' '.join(df['primaryName'].dropna())
wordcloud = WordCloud(width=1200, height=500, background_color='white').generate(names)

fig, ax = plt.subplots(figsize=(14, 6))
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis("off")
st.pyplot(fig)
