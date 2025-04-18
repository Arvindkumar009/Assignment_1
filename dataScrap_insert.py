

pip install pandas streamlit matplotlib seaborn sqlalchemy pymysql
# imdb_scraper.py
import os
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    """Configure Chrome WebDriver with realistic settings"""
    options = webdriver.ChromeOptions()
    
    # Comment this out for debugging
    options.add_argument('--headless')
    
    # Mimic a real browser
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-infobars')
    options.add_argument('--start-maximized')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Initialize driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Mask selenium detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def scrape_imdb_data(max_pages=2):
    """Scrape IMDB movie data with robust error handling"""
    driver = setup_driver()
    base_url = "https://www.imdb.com/search/title/?title_type=feature&release_date=2024-01-01,2024-12-31&sort=num_votes,desc"
    movies_data = []
    
    try:
        for page in range(1, max_pages + 1):
            url = f"{base_url}&start={((page-1)*50)+1}" if page > 1 else base_url
            print(f"\nAttempting to scrape page {page}: {url}")
            
            try:
                # Random delay between requests
                time.sleep(random.uniform(1, 3))
                
                driver.get(url)
                print("Page loaded, waiting for content...")
                
                # Wait for either content or consent banner
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".lister-list"))
                    )
                    print("Main content loaded")
                except TimeoutException:
                    print("Main content not found, trying alternative approach...")
                    # Alternative wait for different page structure
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-metadata-list"))
                    )
                
                # Handle consent banner if present
                try:
                    consent_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
                    consent_button.click()
                    print("Accepted cookies")
                    time.sleep(2)
                except NoSuchElementException:
                    pass
                
                # Scroll to trigger lazy loading
                print("Scrolling to load all content...")
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(0.5, 1.5))
                
                # Find all movie containers
                movie_containers = driver.find_elements(
                    By.CSS_SELECTOR, 
                    ".lister-item.mode-advanced, .ipc-metadata-list-summary-item"
                )
                print(f"Found {len(movie_containers)} movie containers")
                
                for container in movie_containers:
                    try:
                        movie = extract_movie_data(container)
                        if movie:
                            movies_data.append(movie)
                    except Exception as e:
                        print(f"Error processing movie: {str(e)}")
                        continue
                        
                print(f"Successfully scraped {len(movie_containers)} movies from page {page}")
                
            except Exception as e:
                print(f"Error processing page {page}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        driver.quit()
    
    return pd.DataFrame(movies_data)

def extract_movie_data(container):
    """Extract data from a single movie container"""
    try:
        # Modern IMDB structure
        try:
            name = container.find_element(
                By.CSS_SELECTOR, 
                ".lister-item-header a, .ipc-title__text"
            ).text
        except:
            return None
        
        # Handle different rating selectors
        rating = None
        try:
            rating = float(container.find_element(
                By.CSS_SELECTOR, 
                ".ratings-imdb-rating strong, .ipc-rating-star"
            ).text.split()[0])
        except (NoSuchElementException, ValueError):
            pass
        
        # Handle votes
        votes = None
        try:
            votes_text = container.find_element(
                By.CSS_SELECTOR, 
                "p.sort-num_votes-visible span[name='nv'], [data-testid='ratingCount']"
            ).text.replace(',', '')
            votes = int(votes_text) if votes_text.isdigit() else None
        except (NoSuchElementException, ValueError):
            pass
        
        # Handle duration
        duration = None
        try:
            duration_text = container.find_element(
                By.CSS_SELECTOR, 
                ".runtime, .ipc-metadata-list-summary-item__li"
            ).text.split()[0]
            duration = int(duration_text) if duration_text.isdigit() else None
        except (NoSuchElementException, ValueError):
            pass
        
        # Handle genres
        genres = []
        try:
            genre_text = container.find_element(
                By.CSS_SELECTOR, 
                ".genre, .ipc-metadata-list-summary-item__li:last-child"
            ).text
            genres = [g.strip() for g in genre_text.split(',')]
        except NoSuchElementException:
            pass
        
        return {
            'name': name,
            'rating': rating,
            'votes': votes,
            'duration': duration,
            'genres': ', '.join(genres)
        }
        
    except Exception as e:
        print(f"Error extracting movie data: {str(e)}")
        return None

def save_data(df, output_dir='data'):
    """Save data to CSV files"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    if not df.empty:
        # Save all movies
        all_movies_path = os.path.join(output_dir, 'all_movies.csv')
        df.to_csv(all_movies_path, index=False)
        print(f"\nSaved all movies to {all_movies_path}")
        
        # Save by genre
        for genre in set(g for sublist in df['genres'].str.split(', ') for g in sublist if g):
            genre_df = df[df['genres'].str.contains(genre, na=False)]
            safe_genre = genre.lower().replace(' ', '_').replace('/', '_')
            genre_path = os.path.join(output_dir, f'{safe_genre}.csv')
            genre_df.to_csv(genre_path, index=False)
        
        print(f"Saved genre-specific files to {output_dir}")
    else:
        print("No data to save")

if __name__ == "__main__":
    print("Starting IMDB 2024 Movie Scraper...")
    movie_df = scrape_imdb_data(max_pages=2)
    
    if not movie_df.empty:
        print(f"\nSuccessfully collected {len(movie_df)} movies!")
        print(movie_df.head())
        save_data(movie_df)
    else:
        print("\nFailed to collect any movie data. Possible reasons:")
        print("- IMDB blocked the scraper (try again later)")
        print("- Website structure changed (update selectors)")
        print("- Network issues (check your connection)")