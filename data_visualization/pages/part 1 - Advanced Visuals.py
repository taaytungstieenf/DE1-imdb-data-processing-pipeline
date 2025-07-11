import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os
from wordcloud import WordCloud

# Cấu hình Streamlit
st.set_page_config(page_title="📈 Advanced IMDB Visuals", layout="wide")
st.title("📈 Advanced IMDB Data Visualization")

# Cấu hình kết nối MySQL
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

    # Convert về số hợp lệ
    df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
    df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')
    df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
    df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
    df['numVotes'] = pd.to_numeric(df['numVotes'], errors='coerce')

    return df


df = load_data()

# Hiển thị data mẫu
st.subheader("📄 Sample Data (First 5 Rows)")
st.dataframe(df.head(5))

# Biểu đồ: Số lượng dòng theo từng startYear
st.subheader("📅 Number of Movies by Start Year")
year_counts = df['startYear'].dropna().astype(int).value_counts().sort_index()
st.bar_chart(year_counts)

# Bộ lọc người dùng
with st.sidebar:
    st.header("🔍 Filters")
    min_year, max_year = int(df['startYear'].min()), int(df['startYear'].max())
    year_range = st.slider("Select Year Range", min_year, max_year, (2010, 2021))
    genre_options = sorted(set(g for sublist in df['genres'].dropna().str.split(',') for g in sublist))
    selected_genres = st.multiselect("Select Genres", genre_options, default=[])

# Lọc dữ liệu
filtered_df = df[
    (df['startYear'] >= year_range[0]) &
    (df['startYear'] <= year_range[1])
    ]
if selected_genres:
    filtered_df = filtered_df[filtered_df['genres'].notna()]
    filtered_df = filtered_df[filtered_df['genres'].apply(lambda x: any(g in x.split(',') for g in selected_genres))]

# Heatmap tương quan
st.subheader("🔗 Correlation Heatmap")
numeric_cols = ['averageRating', 'runtimeMinutes', 'numVotes']
corr = filtered_df[numeric_cols].corr()
fig, ax = plt.subplots()
sns.heatmap(corr, annot=True, cmap='YlGnBu', ax=ax)
st.pyplot(fig)

# Scatter plot
st.subheader("🎬 Runtime vs Rating")
fig, ax = plt.subplots()
sns.scatterplot(data=filtered_df, x='runtimeMinutes', y='averageRating', alpha=0.6, ax=ax)
ax.set_xlabel("Runtime (minutes)")
ax.set_ylabel("Average Rating")
st.pyplot(fig)

# Boxplot rating theo top genres
st.subheader("🎭 Rating Distribution per Genre (Top 5)")
df_exploded = df.dropna(subset=['genres', 'averageRating']).copy()
df_exploded['averageRating'] = pd.to_numeric(df_exploded['averageRating'], errors='coerce')
df_exploded = df_exploded.dropna(subset=['averageRating'])
df_exploded = df_exploded.assign(genre=df_exploded['genres'].str.split(',')).explode('genre')
top5_genres = df_exploded['genre'].value_counts().nlargest(5).index.tolist()
top_df = df_exploded[df_exploded['genre'].isin(top5_genres)]
fig, ax = plt.subplots(figsize=(10, 5))
sns.boxplot(data=top_df, x='genre', y='averageRating', ax=ax)
st.pyplot(fig)

# Word cloud từ primaryName
st.subheader("☁️ Word Cloud of Popular Names")
names = ' '.join(df['primaryName'].dropna())
wordcloud = WordCloud(width=1000, height=400, background_color='white').generate(names)
fig, ax = plt.subplots(figsize=(12, 5))
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis("off")
st.pyplot(fig)
