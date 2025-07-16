#---------------------------------this file was scrape.py file perviously--------------------------
import json
import time
import re
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
import psycopg2

class ClutchScraper:
    def __init__(self, proxy=None, use_browserstack=False):
        self.proxy = proxy
        self.use_browserstack = use_browserstack
        self.driver = self.setup_driver_clutch()

    def random_human_delay(self, min_sec=5, max_sec=8):
        time.sleep(random.uniform(min_sec, max_sec))

    def move_mouse_randomly(self):
        driver = self.driver
        width = driver.execute_script('return window.innerWidth')
        height = driver.execute_script('return window.innerHeight')
        for _ in range(random.randint(2, 5)):
            x = random.randint(0, width)
            y = random.randint(0, height)
            driver.execute_script(f"window.scrollTo({x}, {y});")
            time.sleep(random.uniform(5, 8))

    def setup_driver_clutch(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--user-data-dir=C:/Temp/ChromeProfile')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--enable-unsafe-swiftshader')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
        options.add_argument('accept-language=en-US,en;q=0.9')
        if self.proxy:
            options.add_argument(f'--proxy-server={self.proxy}')
        try:
            if self.use_browserstack:
                desired_cap = {
                    'os': 'Windows',
                    'os_version': '10',
                    'browser': 'Chrome',
                    'browser_version': 'latest',
                    'name': 'Cloudflare Bypass Test',
                    'build': 'clutch-scraper'
                }
                USERNAME = 'YOUR_BROWSERSTACK_USERNAME'
                ACCESS_KEY = 'YOUR_BROWSERSTACK_ACCESS_KEY'
                url = f'https://{USERNAME}:{ACCESS_KEY}@hub-cloud.browserstack.com/wd/hub'
                driver = webdriver.Remote(
                    command_executor=url,
                    desired_capabilities=desired_cap
                )
            else:
                driver = uc.Chrome(options=options)
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                })
            return driver
        except Exception as e:
            print(f"Error setting up WebDriver: {e}")
            return None

    def scrape_reviews_from_current_page(self, page_num):
        driver = self.driver
        reviews = []
        print(f":mag: Scraping reviews from page {page_num + 1}...")
        self.random_human_delay(2, 5)
        self.move_mouse_randomly()
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        self.random_human_delay(1, 3)
        try:
            review_elements = driver.find_elements(By.CSS_SELECTOR, 'div.profile-review__review')
            if not review_elements:
                print(":x: No review elements found.")
            for element in review_elements:
                review_data = {}
                try:
                    quote_elem = element.find_element(By.CSS_SELECTOR, '.profile-review__quote p')
                    review_text = quote_elem.text.strip()
                    if (review_text.startswith('"') and review_text.endswith('"')) or \
                       (review_text.startswith("'") and review_text.endswith("'")):
                        review_text = review_text[1:-1].strip()
                    review_text = review_text.strip('"\'""\'')

                    # NEW: Try to extract additional feedback text
                    try:
                        feedback_elem = element.find_element(By.XPATH, "following-sibling::div[contains(@class, 'profile-review__feedback')]")
                        p_tags = feedback_elem.find_elements(By.TAG_NAME, 'p')
                        feedback_texts = [p.text.strip() for p in p_tags if p.text.strip()]
                        feedback_text = " ".join(feedback_texts)
                        if feedback_text:
                            review_text = f"{review_text}\n{feedback_text}"
                    except NoSuchElementException:
                        pass  # No additional feedback, continue as normal

                    review_data['text'] = review_text
                    print(f"Extracted review text: {review_text[:100]}...")
                except NoSuchElementException:
                    print("Could not find review text in .profile-review__quote p")
                    continue
                try:
                    date_elem = element.find_element(By.CSS_SELECTOR, '.profile-review__date')
                    review_data['date'] = date_elem.text.strip()
                    print(f"Extracted date: {review_data['date']}")
                except NoSuchElementException:
                    print("Could not find date in .profile-review__date")
                    review_data['date'] = "N/A"
                try:
                    reviewer_elem = element.find_element(
                        By.XPATH,
                        "following-sibling::div[contains(@class, 'profile-review__reviewer')][1]"
                    )
                    name_elem = reviewer_elem.find_element(By.CSS_SELECTOR, '.reviewer_card--name')
                    review_data['name'] = name_elem.text.strip()
                except NoSuchElementException:
                    review_data['name'] = ""
                try:
                    designation_elem = reviewer_elem.find_element(By.CSS_SELECTOR, '.reviewer_position')
                    review_data['designation'] = designation_elem.text.strip()
                except NoSuchElementException:
                    review_data['designation'] = ""
                try:
                    content_elem = element.find_element(By.XPATH, "preceding-sibling::div[contains(@class, 'profile-review__content')][1]")
                    rating_elem = content_elem.find_element(By.CSS_SELECTOR, '.sg-rating.profile-review__rating .sg-rating__number')
                    review_data['user_rating'] = rating_elem.text.strip()
                    print(f"Extracted rating: {review_data['user_rating']}")
                except NoSuchElementException:
                    review_data['user_rating'] = ""
                try:
                    rating_selectors = [
                        '.star-rating',
                        '.rating',
                        '[class*="star"]',
                        '[class*="rating"]'
                    ]
                    for rating_selector in rating_selectors:
                        try:
                            rating_elem = element.find_element(By.CSS_SELECTOR, rating_selector)
                            review_data['user_rating'] = rating_elem.text.strip()
                            print(f"Extracted rating: {review_data['user_rating']}")
                            break
                        except NoSuchElementException:
                            continue
                except Exception as e:
                    print(f"Error extracting rating: {e}")
                    review_data['user_rating'] = "N/A"
                if review_data.get('text'):
                    reviews.append(review_data)
                    print(f":white_check_mark: Added review #{len(reviews)}")
                else:
                    print(":x: Skipped review - no text found")
        except Exception as e:
            print(f"Error extracting reviews on page {page_num + 1}: {e}")
        print(f":dart: Total reviews extracted: {len(reviews)}")
        return reviews

    def get_total_pages(self):
        driver = self.driver
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".sg-pagination-v2"))
            )
            pagination_links = driver.find_elements(By.CSS_SELECTOR, ".sg-pagination-v2-page-number")
            page_numbers = []
            for link in pagination_links:
                try:
                    page_num = int(link.get_attribute("data-page"))
                    page_numbers.append(page_num)
                except (ValueError, TypeError):
                    continue
            if page_numbers:
                total_pages = max(page_numbers) + 1
                print(f":mag: Found {total_pages} total pages (data-page values: {page_numbers})")
                return total_pages
            else:
                print(":page_facing_up: No pagination found, assuming 1 page")
                return 1
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
            print(f"\n:page_facing_up: Processing page 1")
            page_reviews = self.scrape_reviews_from_current_page(0)
            reviews.extend(page_reviews)
            print(f":bar_chart: Total reviews collected so far: {len(reviews)}")
            total_pages = self.get_total_pages()
            print(f":mag: Total pages found: {total_pages}")
            for page in range(1, total_pages):
                print(f"\n:page_facing_up: Processing page {page + 1} of {total_pages}")
                try:
                    next_url = url + f"?page={page}#reviews"
                    driver.get(next_url)
                    print(f":globe_with_meridians: Navigating to: {next_url}")
                    time.sleep(10)
                    if "page=" in next_url:
                        print(f":white_check_mark: Successfully navigated to page {page + 1}")
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(3)
                        page_reviews = self.scrape_reviews_from_current_page(page)
                        reviews.extend(page_reviews)
                    else:
                        print(f":x: Navigation failed for page {page + 1}")
                        break
                except Exception as e:
                    print(f":x: Error navigating to page {page + 1}: {e}")
                    continue
        except Exception as e:
            print(f":x: Error during scraping: {e}")
        print(f"\n:dart: Scraping completed! Total reviews extracted: {len(reviews)}")
        return reviews

    def get_overall_rating(self):
        driver = self.driver
        try:
            rating_elem = driver.find_element(
                By.CSS_SELECTOR, ".profile-metrics__value.profile-metrics__value--rating"
            )
            rating_text = rating_elem.text.strip()
            if "/5" in rating_text:
                rating_text = rating_text.split("/")[0].strip()
            return float(rating_text)
        except NoSuchElementException:
            print("Overall rating element not found.")
            return None
        except Exception as e:
            print(f"Error extracting overall rating: {e}")
            return None

    def close(self):
        if self.driver:
            self.driver.quit()

    def run(self, url, company_id, company_name ):
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
                    "source": "clutch",
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
#     company_url = input("Enter the Clutch company URL: ").strip()
#     company_id = input("Enter the company ID: ").strip()
#     company_name = input("Enter the company name: ").strip()
#     scraper = ClutchScraper()
#     scraped_data = scraper.run(company_url, company_id, company_name)
#     if scraped_data:
#         import json
#         with open('clutch_scraped_data.json', 'w', encoding='utf-8') as f:
#             json.dump(scraped_data, f, ensure_ascii=False, indent=2)
#         print("Scraped data saved to clutch_scraped_data.json")
#     else:
#         print("No data scraped.")
