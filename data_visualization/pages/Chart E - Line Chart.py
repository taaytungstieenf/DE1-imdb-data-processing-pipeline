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
    query = "SELECT startYear, averageRating FROM fact_movies WHERE startYear IS NOT NULL AND averageRating IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
    df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
    df = df.dropna()
    return df

st.title("📊 Line Chart: Avg Rating by Year")

df = load_data()
avg_rating_per_year = df.groupby('startYear')['averageRating'].mean().sort_index()

fig, ax = plt.subplots()
avg_rating_per_year.plot(ax=ax)
ax.set_xlabel('Year')
ax.set_ylabel('Average Rating')
ax.set_title('Average Rating Over Years')
st.pyplot(fig)
