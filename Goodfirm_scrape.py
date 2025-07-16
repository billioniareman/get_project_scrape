import time
import re
import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import psycopg2

class GoodFirmsScraper:
    def __init__(self, headless=False):
        self.driver = self.setup_driver_goodfirms(headless)

    def setup_driver_goodfirms(self, headless=False):
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36")
        driver = uc.Chrome(options=options, use_subprocess=True)
        return driver

    def scrape_reviews_from_current_page(self):
        driver = self.driver
        reviews = []
        time.sleep(5)
        review_elements = driver.find_elements(By.CSS_SELECTOR, 'article.profile-review')
        if not review_elements:
            print(":x: No reviews found on this page.")
            with open("goodfirms_debug.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("Saved HTML to goodfirms_debug.html for inspection.")
        for element in review_elements:
            review_data = {}
            try:
                review_data['name'] = element.find_element(By.CSS_SELECTOR, 'span[itemprop="name"]').text.strip()
            except NoSuchElementException:
                review_data['name'] = "Anonymous"
            try:
                block = element.find_element(By.CSS_SELECTOR, '.reviewer-name').text.strip()
                review_data['designation'] = block.split(",", 1)[1].strip() if ',' in block else ""
            except NoSuchElementException:
                review_data['designation'] = ""
            try:
                date = element.find_element(By.CSS_SELECTOR, '.review-date').text.strip()
                review_data['date'] = date.replace("Posted on", "").strip()
            except NoSuchElementException:
                review_data['date'] = "N/A"
            try:
                title = element.find_element(By.CSS_SELECTOR, 'h3.review-title').text.strip()
                summary = element.find_element(By.CSS_SELECTOR, '.review-summary').text.strip()
                review_data['text'] = f"{title}: {summary}"
            except NoSuchElementException:
                continue
            try:
                rating_items = element.find_elements(By.CSS_SELECTOR, '.review-rating-breakdown-list li')
                for item in rating_items:
                    label = item.find_element(By.CSS_SELECTOR, 'span:first-child').text.strip().lower()
                    if 'overall rating' in label:
                        style = item.find_element(By.CSS_SELECTOR, '.rating-star-container').get_attribute('style')
                        match = re.search(r'width:\s*(\d+)%', style)
                        if match:
                            review_data['user_rating'] = str(round(int(match.group(1)) / 20, 1))
                            break
                else:
                    review_data['user_rating'] = "N/A"
            except NoSuchElementException:
                review_data['user_rating'] = "N/A"
            reviews.append(review_data)
        return reviews

    def scrape_all_pages(self, url):
        driver = self.driver
        reviews = []
        print(f":mag: Starting to scrape ALL reviews from: {url}")
        try:
            driver.get(url)
            time.sleep(5)
            page_number = 1
            while True:
                print(f"\n:page_facing_up: Processing page {page_number}")
                page_reviews = self.scrape_reviews_from_current_page()
                reviews.extend(page_reviews)
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, 'li.next-page a')
                    next_href = next_btn.get_attribute("data-href")
                    if next_href:
                        driver.get(next_href)
                        page_number += 1
                        time.sleep(3)
                    else:
                        print(":x: No more pages.")
                        break
                except NoSuchElementException:
                    print(":x: 'Next Page' not found.")
                    break
        except Exception as e:
            print(f":x: Error during scraping: {e}")
        print(f"\n:dart: Scraping completed! Total reviews extracted: {len(reviews)}")
        return reviews

    def get_overall_rating(self):
        driver = self.driver
        try:
            rating_elem = driver.find_element(
                By.CSS_SELECTOR, "span.review-rating.d-flex[itemprop='ratingValue']"
            )
            rating_text = rating_elem.text.strip()
            return float(rating_text)
        except NoSuchElementException:
            print("GoodFirms overall rating element not found.")
            return None
        except Exception as e:
            print(f"Error extracting GoodFirms overall rating: {e}")
            return None

    def close(self):
        if self.driver:
            self.driver.quit()

    def run(self, url, company_id, company_name):
        driver = self.driver
        if not driver:
            print(":x: Failed to setup WebDriver")
            return None
        try:
            reviews = self.scrape_all_pages(url)
            platform_rating = self.get_overall_rating()
            if reviews:
                result = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "source": "goodfirms",
                    "source_url": url,
                    "platform_rating": platform_rating,
                    "reviews": reviews
                }
                print(f"\n:bar_chart: Summary:")
                print(f"   Total reviews: {len(reviews)}")
                print(f"   Output: Returning scraped data")
                return result
            else:
                print(":x: No reviews found")
                return None
        except Exception as e:
            print(f":x: Error during scraping: {e}")
            return None
        finally:
            self.close()
            print(":end: WebDriver closed")

# if __name__ == "__main__":
#     company_url = input("Enter the GoodFirms company reviews URL: ").strip()
#     company_id = input("Enter the company ID: ").strip()
#     company_name = input("Enter the company name: ").strip()
#     platform_rating = input("Enter the platform rating (or leave blank to auto-detect): ").strip()
#     if not company_url or not company_id or not company_name:
#         print(":x: Missing required input. Exiting.")
#     else:
#         scraper = GoodFirmsScraper()
#         scraper.run(company_url, company_id, company_name)

