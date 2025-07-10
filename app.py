from flask import Flask, jsonify, request
import requests
import logging
from Ambition_scrape import AmbitionBoxScraper
from Clutch_scrape import ClutchScraper
from Goodfirm_scrape import GoodFirmsScraper
app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.route("/scrape", methods=["POST"])
def scrape():
    """Trigger scraping for all companies and sources."""
    try:
        # Fetch company data from API   
        api_url = "http://127.0.0.1:3000/api/v2/getCompanyDataForReviewScrap"
        response = requests.get(api_url)
        print(response.status_code, response.text)  # Debugging line to check API response
        response.raise_for_status()
        resp_json = response.json()
        company = resp_json.get("data")
        if not company or not isinstance(company, dict):
            logger.error("API did not return a valid company object.")
            return jsonify({'error': 'Invalid API response'}), 500
        companies = [company]  # Wrap in a list for compatibility with the rest of the code

        scraper_map = {
            "clutch": (ClutchScraper, "clutch_scrap"),
            "ambitionbox": (AmbitionBoxScraper, "ambitionbox_scrap"),
            "goodfirms": (GoodFirmsScraper, "goodfirms_scrap"),
        }

        for company in companies:
            company_id = company.get("company_id")
            company_name = company.get("company_name")
            reviews_urls = company.get("reviews_urls", [])

            for source in reviews_urls:
                src = source.get('source')
                link = source.get('link')
                if src in scraper_map and link:
                    scraper_cls, scrap_flag = scraper_map[src]
                    if str(company.get(scrap_flag)).lower() != 'true':
                        scraper_cls().run(url=link, company_id=company_id, company_name=company_name)
                    else:
                        logger.info(f"Source '{src}' already scraped for company '{company_name}'")
        return jsonify({'message': 'Scraping triggered'}), 200
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
