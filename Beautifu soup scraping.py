import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas_gbq
from google.cloud import bigquery

# Query BigQuery historical table to get existing article URLs using google-cloud-bigquery
def get_existing_article_urls(project_id, historical_table):
    bq_client = bigquery.Client(project=project_id)
    # Fully qualified table name is expected here
    query = f"SELECT `Article URL` FROM `{historical_table}`"
    try:
        df = bq_client.query(query).to_dataframe()
        existing_urls = df['Article URL'].tolist()
        return existing_urls
    except Exception as e:
        print(f"Error fetching existing articles from BigQuery: {e}")
        return []

# Scrape article links from the main page using requests and BeautifulSoup
def scrape_article_links(base_url, limit=20):
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching base URL {base_url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    articles = []
    # Find all <a> tags with rel="bookmark"
    links = soup.find_all("a", rel="bookmark")
    for link in links[:limit]:
        title = link.get_text(strip=True)
        href = link.get("href")
        articles.append({
            "Article Title": title,
            "Article URL": href
        })
    return articles

# Scrape detailed data from each article using BeautifulSoup
def scrape_article_details(article):
    url = article["Article URL"]
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching article {url}: {e}")
        article["Categories"] = []
        article["Author"] = None
        article["Updated On"] = None
        return article

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract categories using the CSS selector ".article-breadcrumbs a"
    cat_elements = soup.select(".article-breadcrumbs a")
    article["Categories"] = [cat.get_text(strip=True) for cat in cat_elements]

    # Extract author using the CSS selector ".entry-authors .author a"
    author_element = soup.select_one(".entry-authors .author a")
    article["Author"] = author_element.get_text(strip=True) if author_element else None

    # Extract publication date using the CSS selector ".entry-header__updated-on"
    date_element = soup.select_one(".entry-header__updated-on")
    if date_element:
        date_text = date_element.get_text(strip=True)
        # Remove prefix "Updated on " if present
        if date_text.startswith("Updated on "):
            date_text = date_text.replace("Updated on ", "", 1)
        article["Updated On"] = date_text
    else:
        article["Updated On"] = None

    print(f"Article '{article['Article Title']}' scraped successfully.")
    return article

# Save data to BigQuery using pandas_gbq
def save_to_bigquery(data, destination_table, project_id):
    try:
        df = pd.DataFrame(data)
        # Add a scrape timestamp for auditing purposes
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
    base_url = "https://www.additudemag.com/"
    project_id = "data-444203"  # Update with your correct project_id
    # Fully qualified table names
    historical_table = f"{project_id}.addtitude.historical"
    streaming_table = f"{project_id}.addtitude.streaming"

    # Step 1: Get existing article URLs from the BigQuery historical table.
    existing_urls = get_existing_article_urls(project_id, historical_table)
    print(f"Found {len(existing_urls)} existing articles in historical table.")

    # Step 2: Scrape article links from the main page (limit increased to 20).
    articles = scrape_article_links(base_url, limit=20)
    print(f"{len(articles)} articles found on the main page.")

    # Step 3: Filter out articles that already exist in the historical table.
    new_articles = [article for article in articles if article["Article URL"] not in existing_urls]
    print(f"{len(new_articles)} new articles to scrape and insert into the streaming table.")

    # Step 4: For each new article, scrape details using BeautifulSoup.
    detailed_articles = []
    for article in new_articles:
        detailed_article = scrape_article_details(article)
        detailed_articles.append(detailed_article)

    # Step 5: Save the new detailed articles to the streaming table.
    if detailed_articles:
        save_to_bigquery(detailed_articles, streaming_table, project_id)
    else:
        print("No new articles to scrape.")

if __name__ == "__main__":
    main()
