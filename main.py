import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup Chrome WebDriver
def setup_driver():
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    wait = WebDriverWait(driver, 10)
    return driver, wait

# Navigate to the main page and extract article links
def scrape_article_links(driver, base_url, limit=5):
    driver.get(base_url)
    time.sleep(3)  # Allow page to load
    articles = []
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[rel="bookmark"]'))
        )
        links = driver.find_elements(By.CSS_SELECTOR, 'a[rel="bookmark"]')
        
        for link in links[:limit]:  # Limit the number of articles to scrape
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
            time.sleep(3)  # Allow article page to load
            
            # Extract categories
            categories = driver.find_elements(By.CSS_SELECTOR, '.article-breadcrumbs a')
            article['Categories'] = [cat.text for cat in categories]
            
            # Extract author
            try:
                author = driver.find_element(By.CSS_SELECTOR, '.entry-authors .author a').text
                article['Author'] = author
            except:
                article['Author'] = None
            
            # Extract publication date
            try:
                date = driver.find_element(By.CSS_SELECTOR, '.entry-header__updated-on').text.replace("Updated on ", "")
                article['Updated On'] = date
            except:
                article['Updated On'] = None
            
            print(f"Article '{article['Article Title']}' scraped successfully.")
        except Exception as e:
            print(f"Error fetching details for {article['Article Title']}: {e}")
            article['Categories'] = []
            article['Author'] = None
            article['Updated On'] = None
    
    return articles

# Save data to a CSV file
def save_to_csv(data, file_name):
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}.")
    except Exception as e:
        print(f"Error saving data: {e}")

# Main function to orchestrate the scraping
def main():
    base_url = "https://www.additudemag.com/"
    output_file = "additude_articles_detailed.csv"
    
    driver, wait = setup_driver()
    
    try:
        # Step 1: Scrape article links from the main page (limit 5 articles)
        articles = scrape_article_links(driver, base_url, limit=5)
        print(f"{len(articles)} articles found on the main page.")
        
        # Step 2: Scrape details for each article
        detailed_articles = scrape_article_details(driver, articles)
        
        # Step 3: Save data to CSV
        save_to_csv(detailed_articles, output_file)
    
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
