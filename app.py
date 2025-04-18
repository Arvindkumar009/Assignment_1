# app.py  
import streamlit as st  
import pandas as pd  
from sqlalchemy import create_engine  

# For Streamlit >=1.18.0 use:  
#@st.cache_resource  
# For older versions use:  
@st.experimental_singleton  
def get_db_connection():  
    # MySQL connection details  
    db_config = {  
        'user': 'root',  
        'password': 'admin',  
        'host': 'localhost',  
        'database': 'imb_2024',  
        'port': 3306  
    }  
    connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"  
    return create_engine(connection_string)  

# For Streamlit >=1.18.0 use:  
#@st.cache_data  
# For older versions use:  
@st.experimental_memo  
def load_data():  
    try:  
        engine = get_db_connection()  
        movies_df = pd.read_sql("SELECT * FROM movies", engine)  
        genre_stats = pd.read_sql("SELECT * FROM genre_stats", engine)  
        top_rated = pd.read_sql("SELECT * FROM top_rated_movies", engine)  
        return movies_df, genre_stats, top_rated  
    except Exception as e:  
        st.error(f"Error loading data: {str(e)}")  
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()  

def main():  
    st.title("IMDB 2024 Movie Analysis")  
    
    # Load data  
    movies_df, genre_stats, top_rated = load_data()  
    
    if movies_df.empty:  
        st.warning("No movie data available. Please check your database connection.")  
        return  
    
    # Sidebar filters  
    st.sidebar.header("Filters")  
    
    # Genre filter  
    try:  
        all_genres = sorted(set(g for sublist in movies_df['genres'].str.split(', ') for g in sublist if g))  
        selected_genres = st.sidebar.multiselect("Select Genres", all_genres)  
    except:  
        selected_genres = []  
    
    # Rating filter  
    min_rating, max_rating = st.sidebar.slider(  
        "Rating Range",  
        min_value=0.0,  
        max_value=10.0,  
        value=(movies_df['rating'].min(), movies_df['rating'].max()),  
        step=0.1  
    )  
    
    # Duration filter  
    duration_options = ['All'] + sorted(movies_df['duration_category'].unique())  
    selected_duration = st.sidebar.selectbox("Duration Category", duration_options)  
    
    # Votes filter  
    min_votes = st.sidebar.number_input("Minimum Votes",   
                                      min_value=0,   
                                      value=0,  
                                      max_value=int(movies_df['votes'].max()))  
    
    # Apply filters  
    filtered_df = movies_df.copy()  
    
    if selected_genres:  
        filtered_df = filtered_df[filtered_df['genres'].str.contains('|'.join(selected_genres), na=False)]  
    
    filtered_df = filtered_df[  
        (filtered_df['rating'] >= min_rating) &   
        (filtered_df['rating'] <= max_rating) &  
        (filtered_df['votes'] >= min_votes)  
    ]  
    
    if selected_duration != 'All':  
        filtered_df = filtered_df[filtered_df['duration_category'] == selected_duration]  
    
    # Display filtered data  
    st.subheader("Filtered Movies")  
    st.dataframe(filtered_df)  
    
    # Analysis tabs  
    tab1, tab2, tab3 = st.tabs(["Top Movies", "Genre Analysis", "Duration Insights"])  
    
    with tab1:  
        st.subheader("Top 10 Rated Movies")  
        st.dataframe(top_rated)  
        
        st.subheader("Top 10 Most Voted Movies")  
        try:  
            top_voted = pd.read_sql(  
                "SELECT name, votes, rating, duration, genres FROM movies ORDER BY votes DESC LIMIT 10",   
                get_db_connection()  
            )  
            st.dataframe(top_voted)  
        except Exception as e:  
            st.error(f"Couldn't load most voted movies: {str(e)}")  
    
    with tab2:  
        st.subheader("Genre Statistics")  
        st.dataframe(genre_stats)  
        
        col1, col2 = st.columns(2)  
        with col1:  
            st.bar_chart(genre_stats.set_index('genre')['movie_count'])  
        with col2:  
            st.bar_chart(genre_stats.set_index('genre')['avg_rating'])  
    
    with tab3:  
        st.subheader("Duration Distribution")  
        duration_counts = filtered_df['duration_category'].value_counts().sort_index()  
        st.bar_chart(duration_counts)  
        
        col1, col2 = st.columns(2)  
        with col1:  
            st.subheader("Shortest Movies")  
            st.dataframe(filtered_df.sort_values('duration').head(10))  
        with col2:  
            st.subheader("Longest Movies")  
            st.dataframe(filtered_df.sort_values('duration', ascending=False).head(10))  

if __name__ == "__main__":  
    main()  