import json
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
import psycopg2

class AmbitionBoxScraper:
    def __init__(self, headless=True):
        self.driver = self.setup_driver_ambitionb(headless)

    def setup_driver_ambitionb(self, headless=True):
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        try:
            driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                  Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                  })
                """
            })
            return driver
        except Exception as e:
            print(f"Error setting up WebDriver: {e}")
            print("Please ensure Chrome is installed and webdriver-manager can access the internet.")
            return None

    def scrape_reviews_from_current_page(self):
        driver = self.driver
        reviews = []
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[id^="review-"]')
        if not review_elements:
            print(":x: No review elements found.")
        for element in review_elements:
            review_data = {}
            try:
                likes_elem = element.find_element(By.XPATH, ".//h3[text()='Likes']/following-sibling::p[1]")
                likes_text = likes_elem.text.strip()
            except NoSuchElementException:
                likes_text = ""
            try:
                dislikes_elem = element.find_element(By.XPATH, ".//h3[text()='Dislikes']/following-sibling::p[1]")
                dislikes_text = dislikes_elem.text.strip()
            except NoSuchElementException:
                dislikes_text = ""
            review_text_parts = []
            if likes_text:
                review_text_parts.append(f"Likes: {likes_text}")
            if dislikes_text:
                review_text_parts.append(f"Dislikes: {dislikes_text}")
            if review_text_parts:
                review_text = " ".join(review_text_parts)
                review_text = re.sub(r'\s+', ' ', review_text)
                review_text = review_text.strip()
                review_data['text'] = review_text
            else:
                continue
            try:
                date_elem = element.find_element(By.CSS_SELECTOR, 'span.text-xs.leading-\\[1\\.33\\]')
                date_text = date_elem.text.strip()
                if "updated on" in date_text:
                    review_data['date'] = date_text.replace("updated on", "").strip()
                else:
                    review_data['date'] = date_text
            except NoSuchElementException:
                try:
                    date_elem = element.find_element(By.XPATH, ".//span[contains(text(), 'updated on')]")
                    date_text = date_elem.text.strip()
                    review_data['date'] = date_text.replace("updated on", "").strip()
                except NoSuchElementException:
                    review_data['date'] = "N/A"
            try:
                designation_elem = element.find_element(By.CSS_SELECTOR, 'h2[itemprop="name"]')
                review_data['designation'] = designation_elem.text.strip()
            except NoSuchElementException:
                try:
                    designation_elem = element.find_element(By.CSS_SELECTOR, 'h2.text-lg')
                    review_data['designation'] = designation_elem.text.strip()
                except NoSuchElementException:
                    review_data['designation'] = ""
            review_data['name'] = "Anonymous"
            try:
                rating_elem = element.find_element(By.CSS_SELECTOR, 'span.text-primary-text.font-pn-700.text-sm')
                review_data['user_rating'] = rating_elem.text.strip()
            except NoSuchElementException:
                try:
                    rating_elem = element.find_element(By.CSS_SELECTOR, 'span.font-pn-700')
                    rating_text = rating_elem.text.strip()
                    if rating_text.replace('.', '', 1).isdigit():
                        review_data['user_rating'] = rating_text
                    else:
                        review_data['user_rating'] = "N/A"
                except NoSuchElementException:
                    review_data['user_rating'] = "N/A"
            reviews.append(review_data)
        return reviews

    def get_total_pages(self):
        driver = self.driver
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".pagination-btn.next"))
            )
            page = 1
            while True:
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, ".pagination-btn.next")
                    if "disabled" in next_btn.get_attribute("class"):
                        break
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(3)
                    page += 1
                except (TimeoutException, NoSuchElementException):
                    break
            return page
        except Exception as e:
            print(f":x: Error getting total pages: {e}")
            return 1

    def scrape_all_pages(self, url):
        driver = self.driver
        reviews = []
        print(f":mag: Starting to scrape ALL reviews from: {url}")
        try:
            driver.get(url)
            time.sleep(5)
            page_num = 1
            while True:
                print(f"\n:page_facing_up: Processing page {page_num}")
                page_reviews = self.scrape_reviews_from_current_page()
                reviews.extend(page_reviews)
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, ".pagination-btn.next")
                    if "disabled" in next_btn.get_attribute("class"):
                        print(":x: No more pages.")
                        break
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(3)
                    page_num += 1
                except (TimeoutException, NoSuchElementException):
                    print(":x: No more pages.")
                    break
        except Exception as e:
            print(f":x: Error during scraping: {e}")
        print(f"\n:dart: Scraping completed! Total reviews extracted: {len(reviews)}")
        return reviews

    def get_overall_rating(self):
        driver = self.driver
        try:
            rating_elem = driver.find_element(
                By.CSS_SELECTOR, "span.text-primary-text.font-pn-700"
            )
            rating_text = rating_elem.text.strip()
            return float(rating_text)
        except NoSuchElementException:
            print("AmbitionBox overall rating element not found.")
            return None
        except Exception as e:
            print(f"Error extracting AmbitionBox overall rating: {e}")
            return None

    def close(self):
        if self.driver:
            self.driver.quit()

   
    def run(self, url, company_id, company_name):
        driver = self.driver
        if not driver:
            print(":x: Failed to setup WebDriver")
            return
        try:
            reviews = self.scrape_all_pages(url)
            platform_rating = self.get_overall_rating()
            if reviews:
                result = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "source": "ambitionbox",
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
# if __name__ == '__main__':
#     company_url = input("Enter the AmbitionBox company reviews URL: ").strip()
#     company_id = input("Enter the company ID: ").strip()
#     company_name = input("Enter the company name: ").strip()
#     platform_rating = input("Enter the platform rating (or leave blank to auto-detect): ").strip()
#     if not company_url or not company_id or not company_name:
#         print(":x: Missing required input. Exiting.")
#     else:
#         if not platform_rating:
#             scraper = AmbitionBoxScraper()
#             scraper.driver.get(company_url)
#             platform_rating = scraper.get_overall_rating()
#             scraper.close()
#             scraper = AmbitionBoxScraper()
#             scraper.run(company_url, company_id, company_name, platform_rating)
#         else:
#             scraper = AmbitionBoxScraper()
#             scraper.run(company_url, company_id, company_name, platform_rating)