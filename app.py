from flask import Flask, jsonify, request
import requests
import logging
from Ambition_scrape import AmbitionBoxScraper
from Clutch_scrape import ClutchScraper
from Goodfirm_scrape import GoodFirmsScraper
app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_reviews_to_api(data):
    api_url = "http://127.0.0.1:3000/api/v2/save-company-reviews"
    try:
        response = requests.post(api_url, json=data)
        response.raise_for_status()
        logger.info(f"Sent reviews to API. Response: {response.text}")
        return True
    except Exception as e:
        logger.error(f"Error sending to API: {e}")
        return False

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
        companies = resp_json.get("data")
        if not companies:
            logger.error("API did not return company data.")
            return jsonify({'error': 'Invalid API response'}), 500
        if isinstance(companies, dict):
            companies = [companies]
        elif not isinstance(companies, list):
            logger.error("API did not return a valid company object or list.")
            return jsonify({'error': 'Invalid API response'}), 500

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
                        scraped_data = scraper_cls().run(url=link, company_id=company_id, company_name=company_name)
                        logger.info("============================scraped_data============================",scraped_data)
                        if scraped_data:    
                            send_reviews_to_api(scraped_data)
                            logger.info("============================scraped_data sent to api============================",scraped_data)
                    else:
                        logger.info(f"Source '{src}' already scraped for company '{company_name}'")
        return jsonify({'message': 'Scraping triggered'}), 200
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        return jsonify({'error': str(e)}), 500


# @app.route("/scrape/clutch", methods=["POST"])
# def scrape_clutch():
#     """Trigger scraping for Clutch only."""
#     try:
#         api_url = "http://127.0.0.1:3000/api/v2/getCompanyDataForReviewScrap"
#         response = requests.get(api_url)
#         response.raise_for_status()
#         resp_json = response.json()
#         company = resp_json.get("data")
#         if not company or not isinstance(company, dict):
#             logger.error("API did not return a valid company object.")
#             return jsonify({'error': 'Invalid API response'}), 500
#         companies = [company]

#         for company in companies:
#             company_id = company.get("company_id")
#             company_name = company.get("company_name")
#             reviews_urls = company.get("reviews_urls", [])
#             for source in reviews_urls:
#                 src = source.get('source')
#                 link = source.get('link')
#                 if src == "clutch" and link:
#                     if str(company.get("clutch_scrap")).lower() != 'true':
#                         scraped_data = ClutchScraper().run(url=link, company_id=company_id, company_name=company_name)
#                         if scraped_data:
#                             send_reviews_to_api(scraped_data)
#                     else:
#                         logger.info(f"Clutch already scraped for company '{company_name}'")
#         return jsonify({'message': 'Clutch scraping triggered'}), 200
#     except Exception as e:
#         logger.error(f"Error during Clutch scraping: {e}")
#         return jsonify({'error': str(e)}), 500

# @app.route("/scrape/ambitionbox", methods=["POST"])
# def scrape_ambitionbox():
#     """Trigger scraping for AmbitionBox only."""
#     try:
#         api_url = "http://localhost:3000/api/v2/getCompanyDataForReviewScrap"
#         response = requests.get(api_url)
#         response.raise_for_status()
#         resp_json = response.json()
#         company = resp_json.get("data")
#         if not company or not isinstance(company, dict):
#             logger.error("API did not return a valid company object.")
#             return jsonify({'error': 'Invalid API response'}), 500
#         companies = [company]

#         for company in companies:
#             company_id = company.get("company_id")
#             company_name = company.get("company_name")
#             reviews_urls = company.get("reviews_urls", [])
#             for source in reviews_urls:
#                 src = source.get('source')
#                 link = source.get('link')
#                 if src == "ambitionbox" and link:
#                     if str(company.get("ambitionbox_scrap")).lower() != 'true':
#                         scraped_data = AmbitionBoxScraper().run(url=link, company_id=company_id, company_name=company_name)
#                         if scraped_data:
#                             send_reviews_to_api(scraped_data)
#                     else:
#                         logger.info(f"AmbitionBox already scraped for company '{company_name}'")
#         return jsonify({'message': 'AmbitionBox scraping triggered'}), 200
#     except Exception as e:
#         logger.error(f"Error during AmbitionBox scraping: {e}")
#         return jsonify({'error': str(e)}), 500

# @app.route("/scrape/goodfirms", methods=["POST"])
# def scrape_goodfirms():
#     """Trigger scraping for GoodFirms only."""
#     try:
#         api_url = "http://127.0.0.1:3000/api/v2/getCompanyDataForReviewScrap"
#         response = requests.get(api_url)
#         response.raise_for_status()
#         resp_json = response.json()
#         company = resp_json.get("data")
#         if not company or not isinstance(company, dict):
#             logger.error("API did not return a valid company object.")
#             return jsonify({'error': 'Invalid API response'}), 500
#         companies = [company]

#         for company in companies:
#             company_id = company.get("company_id")
#             company_name = company.get("company_name")
#             reviews_urls = company.get("reviews_urls", [])
#             for source in reviews_urls:
#                 src = source.get('source')
#                 link = source.get('link')
#                 if src == "goodfirms" and link:
#                     if str(company.get("goodfirms_scrap")).lower() != 'true':
#                         scraped_data = GoodFirmsScraper().run(url=link, company_id=company_id, company_name=company_name)
#                         if scraped_data:
#                             send_reviews_to_api(scraped_data)
#                     else:
#                         logger.info(f"GoodFirms already scraped for company '{company_name}'")
#         return jsonify({'message': 'GoodFirms scraping triggered'}), 200
#     except Exception as e:
#         logger.error(f"Error during GoodFirms scraping: {e}")
#         return jsonify({'error': str(e)}), 500



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
