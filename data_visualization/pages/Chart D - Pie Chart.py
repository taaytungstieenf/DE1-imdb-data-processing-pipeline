import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
import os
from collections import Counter

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}

def load_data():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = "SELECT genres FROM fact_movies WHERE genres IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="IMDB Overview", layout="centered")
st.title("📊 Pie Chart: Genre Distribution")

df = load_data()
genre_list = df['genres'].str.split(',').sum()
top_genres = Counter(genre_list).most_common(8)
labels, sizes = zip(*top_genres)

fig, ax = plt.subplots()
ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
ax.axis('equal')
st.pyplot(fig)
