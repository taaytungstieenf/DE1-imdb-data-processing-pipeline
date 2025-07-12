import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '246357'),
    'database': os.getenv('DB_NAME', 'imdbDB'),
}

def load_data():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = "SELECT genres, averageRating FROM fact_movies WHERE genres IS NOT NULL AND averageRating IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
    df = df.dropna()
    # keep only the first category if there are multiple
    df['genres'] = df['genres'].apply(lambda x: x.split(',')[0] if ',' in x else x)
    return df

st.title("📊 Box Plot: Rating by Genre")

df = load_data()

fig, ax = plt.subplots(figsize=(12, 6))
sns.boxplot(x='genres', y='averageRating', data=df, ax=ax)
plt.xticks(rotation=45)
st.pyplot(fig)
