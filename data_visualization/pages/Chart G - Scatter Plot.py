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
    query = "SELECT averageRating, numVotes FROM fact_movies"
    df = pd.read_sql(query, conn)
    conn.close()
    df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
    df['numVotes'] = pd.to_numeric(df['numVotes'], errors='coerce')
    return df.dropna()

st.title("📊 Scatter Plot: Rating vs Number of Votes")

df = load_data()

fig, ax = plt.subplots()
ax.scatter(df['numVotes'], df['averageRating'], alpha=0.5)
ax.set_xlabel('Number of Votes')
ax.set_ylabel('Average Rating')
ax.set_title('Rating vs Votes')
ax.set_xscale('log')  # scale votes for visibility
st.pyplot(fig)
