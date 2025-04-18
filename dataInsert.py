import pandas as pd  
import pymysql  
from sqlalchemy import create_engine, types as sa_types  # Updated import  
import numpy as np  


def clean_data(df):  
    """  
    Clean the scraped movie data with robust NaN handling  
    """  
    # Ensure we're working with a copy  
    df = df.copy()  
    
    # Handle missing ratings - fill with median (better than mean for skewed data)  
    df['rating'] = df['rating'].fillna(df['rating'].median())  
    
    # Handle votes - fill NaN with 0 and ensure int type  
    df['votes'] = pd.to_numeric(df['votes'], errors='coerce').fillna(0).astype(int)  
    
    # Handle duration - more robust cleaning  
    try:  
        # First convert to numeric, coercing errors to NaN  
        df['duration'] = pd.to_numeric(df['duration'], errors='coerce')  
        
        # Replace NaN with median duration (excluding NaN values in calculation)  
        duration_median = df['duration'].median(skipna=True)  
        
        # If all durations are NaN, use a default value (e.g., 120 minutes)  
        if pd.isna(duration_median):  
            duration_median = 120  
            
        df['duration'] = df['duration'].fillna(duration_median)  
        
        # Convert to int - now safely since no NaN/inf values remain  
        df['duration'] = df['duration'].astype(int)  
        
    except Exception as e:  
        print(f"Error processing duration: {e}")  
        # Fallback to default duration if conversion fails  
        df['duration'] = 120  
    
    # Create duration categories - only after ensuring clean duration values  
    bins = [0, 60, 120, 180, np.inf]  
    labels = ['<1h', '1-2h', '2-3h', '>3h']  
    df['duration_category'] = pd.cut(  
        df['duration'],  
        bins=bins,  
        labels=labels,  
        right=False  
    )  
    
    return df  

def store_in_mysql(df):  
    """Store cleaned data in MySQL database with error handling"""  
    db_config = {  
        'user': 'root',  
        'password': 'admin',  
        'host': 'localhost',  
        'port': 3306,  
        'database': 'imb_2024'  
    }  
    
    try:  
        # Create SQLAlchemy engine  
        connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"  
        engine = create_engine(connection_string)  
        
        # Create database if not exists  
        with engine.connect() as conn:  
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")  
            conn.execute(f"USE {db_config['database']}")  
        
        # Store data with proper SQLAlchemy types  
        df.to_sql(  
            name='movies',  
            con=engine,  
            if_exists='replace',  
            index=False,  
            dtype={  
                'name': sa_types.String(length=255),  
                'rating': sa_types.Float(),  
                'votes': sa_types.Integer(),  
                'duration': sa_types.Integer(),  
                'genres': sa_types.String(length=255),  
                'duration_category': sa_types.String(length=10)  
            }  
        )  
          
        
        # Create views for analysis  
        with engine.connect() as conn:  
            # Top rated movies view  
            conn.execute("""  
            CREATE OR REPLACE VIEW top_rated_movies AS  
            SELECT name, rating, votes, duration, genres  
            FROM movies  
            WHERE rating IS NOT NULL  
            ORDER BY rating DESC  
            LIMIT 10  
            """)  
            
            # Genre statistics view (using MySQL 8.0+ recursive CTE)  
            conn.execute("""  
            CREATE OR REPLACE VIEW genre_stats AS  
            WITH RECURSIVE split_genres AS (  
                SELECT   
                    name,   
                    rating,   
                    votes,   
                    duration,  
                    SUBSTRING_INDEX(genres, ', ', 1) AS genre,  
                    CASE   
                        WHEN LOCATE(', ', genres) > 0   
                        THEN SUBSTRING(genres, LOCATE(', ', genres) + 2)  
                        ELSE ''  
                    END AS remaining  
                FROM movies  
                
                UNION ALL  
                
                SELECT  
                    name,  
                    rating,  
                    votes,  
                    duration,  
                    SUBSTRING_INDEX(remaining, ', ', 1) AS genre,  
                    CASE   
                        WHEN LOCATE(', ', remaining) > 0   
                        THEN SUBSTRING(remaining, LOCATE(', ', remaining) + 2)  
                        ELSE ''  
                    END AS remaining  
                FROM split_genres  
                WHERE remaining != ''  
            )  
            SELECT   
                TRIM(genre) AS genre,  
                COUNT(*) AS movie_count,  
                AVG(rating) AS avg_rating,  
                AVG(duration) AS avg_duration,  
                SUM(votes) AS total_votes  
            FROM split_genres  
            WHERE genre != ''  
            GROUP BY TRIM(genre)  
            ORDER BY movie_count DESC  
            """)  
        
        print("Data successfully stored in MySQL with views created.")  
        
    except Exception as e:  
        print(f"Error storing data in MySQL: {str(e)}")  
        raise  

if __name__ == "__main__":  
    try:  
        # Load scraped data  
        df = pd.read_csv('data/all_movies.csv')  
        
        # Clean data with robust error handling  
        cleaned_df = clean_data(df)  
        
        # Verify we have valid data before storing  
        if cleaned_df.empty:  
            raise ValueError("No valid data after cleaning")  
            
        print("Data cleaning completed successfully. Sample data:")  
        print(cleaned_df.head())  
        
        # Store in MySQL  
        store_in_mysql(cleaned_df)  
        
    except Exception as e:  
        print(f"Error in data processing pipeline: {str(e)}")  