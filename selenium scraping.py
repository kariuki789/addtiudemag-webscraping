import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas_gbq
from google.cloud import bigquery

# Setup Chrome WebDriver
def setup_driver():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    wait = WebDriverWait(driver, 10)
    return driver, wait

# Query BigQuery historical table to get existing article URLs using google-cloud-bigquery
def get_existing_article_urls(project_id, historical_table):
    bq_client = bigquery.Client(project=project_id)
    # Use the fully qualified table name if needed; adjust as per your project settings.
    query = f"SELECT `Article URL` FROM `{historical_table}`"
    try:
        df = bq_client.query(query).to_dataframe()
        existing_urls = df['Article URL'].tolist()
        return existing_urls
    except Exception as e:
        print(f"Error fetching existing articles from BigQuery: {e}")
        return []

# Navigate to the main page and extract article links (limit increased to 20)
def scrape_article_links(driver, base_url, limit=20):
    driver.get(base_url)
    time.sleep(3)  # Allow page to load
    articles = []
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[rel="bookmark"]'))
        )
        links = driver.find_elements(By.CSS_SELECTOR, 'a[rel="bookmark"]')
        for link in links[:limit]:
            articles.append({
                'Article Title': link.text.strip(),
                'Article URL': link.get_attribute('href')
            })
    except Exception as e:
        print(f"Error fetching article links: {e}")
    return articles

# Scrape detailed data from each article
def scrape_article_details(driver, articles):
    for article in articles:
        print(f"Scraping article: {article['Article Title']}")
        try:
            driver.get(article['Article URL'])
            time.sleep(3)  # Allow page to load

            # Extract categories
            categories = driver.find_elements(By.CSS_SELECTOR, '.article-breadcrumbs a')
            article['Categories'] = [cat.text for cat in categories]

            # Extract author
            try:
                author = driver.find_element(By.CSS_SELECTOR, '.entry-authors .author a').text
                article['Author'] = author
            except Exception:
                article['Author'] = None

            # Extract publication date
            try:
                date = driver.find_element(By.CSS_SELECTOR, '.entry-header__updated-on').text.replace("Updated on ", "")
                article['Updated On'] = date
            except Exception:
                article['Updated On'] = None

            print(f"Article '{article['Article Title']}' scraped successfully.")
        except Exception as e:
            print(f"Error fetching details for {article['Article Title']}: {e}")
            article['Categories'] = []
            article['Author'] = None
            article['Updated On'] = None
    return articles

# Save data to BigQuery using pandas_gbq
def save_to_bigquery(data, destination_table, project_id):
    try:
        df = pd.DataFrame(data)
        # Optionally, add a scrape timestamp column for auditing purposes
        df['scraped_at'] = pd.Timestamp.now()
        pandas_gbq.to_gbq(
                df,
                destination_table,
                project_id=project_id,
                if_exists='append',
                progress_bar=True
            )
        print(f"Data saved to BigQuery table {destination_table}.")
    except Exception as e:
        print(f"Error saving data to BigQuery: {e}")

# Main function to orchestrate the scraping and ingestion
def main():
    # Define base URL and project/table details.
    base_url = "https://www.additudemag.com/"
    project_id = "data-444203"
    # Fully qualified table names. Adjust if needed.
    historical_table = f"{project_id}.addtitude.historical"
    streaming_table = f"{project_id}.addtitude.streaming"
    
    driver, wait = setup_driver()
    
    try:
        # Step 1: Get existing article URLs from the BigQuery historical table.
        existing_urls = get_existing_article_urls(project_id, historical_table)
        print(f"Found {len(existing_urls)} existing articles in BigQuery historical table.")

        # Step 2: Scrape article links from the main page (limit increased to 20).
        articles = scrape_article_links(driver, base_url, limit=20)
        print(f"{len(articles)} articles found on the main page.")

        # Step 3: Filter out articles that already exist in the historical table.
        new_articles = [article for article in articles if article['Article URL'] not in existing_urls]
        print(f"{len(new_articles)} new articles to scrape and insert into the streaming table.")

        # Step 4: If new articles are found, scrape their details.
        if new_articles:
            detailed_articles = scrape_article_details(driver, new_articles)
            # Step 5: Save the new detailed articles to the streaming table.
            save_to_bigquery(detailed_articles, streaming_table, project_id)
        else:
            print("No new articles to scrape.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
