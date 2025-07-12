import streamlit as st
import pandas as pd
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
    query = "SELECT startYear FROM fact_movies WHERE startYear IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
    return df.dropna()

st.title("📊 Bar Chart: Number of Movies by Start Year")

df = load_data()
top_years = df['startYear'].value_counts().sort_index()

fig, ax = plt.subplots(figsize=(12, 6))
top_years.plot(kind='bar', ax=ax, color='orange')
ax.set_xlabel('Start Year')
ax.set_ylabel('Number of Movies')
ax.set_title('Number of Movies by Start Year')

st.pyplot(fig)
