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
    query = "SELECT averageRating FROM fact_movies WHERE averageRating IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
    return df.dropna()

st.title("📊 Histogram: Average Rating")

df = load_data()

fig, ax = plt.subplots()
ax.hist(df['averageRating'], bins=20, color='skyblue', edgecolor='black')
ax.set_xlabel('Average Rating')
ax.set_ylabel('Frequency')
ax.set_title('Distribution of Average Ratings')

st.pyplot(fig)
