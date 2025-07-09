from flask import Flask, jsonify
from dotenv import load_dotenv
import os
import sys
import asyncio
import psycopg2
import logging

# Scraper imports
from Ambition_scrape import AmbitionBoxScraper
from Clutch_scrape import ClutchScraper
from Goodfirm_scrape import GoodFirmsScraper

# Set event loop policy for Windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Load environment variables
load_dotenv()
URL = os.environ.get("BASE_URL")

# Flask app and blueprint setup
app= Flask(__name__)
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL credentials
PG_HOST = "ss-stag-dev-db-paij5iezee.supersourcing.com"
PG_PORT = 5432
PG_DBNAME = "bluerang_test_master_db"
PG_USER = "bluerangZbEbusr"
PG_PASSWORD = "Year#2015eba"

def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

@app.route("/scrape", methods=["POST"])
def scrape():
    """Trigger scraping for all companies and sources."""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn:
            with conn.cursor() as cur:
                # Only fetch companies where at least one platform is not scraped
                cur.execute("""
                    SELECT company_id, company_name, reviews_urls, clutch_scrap, ambitionbox_scrap, goodfirms_scrap
                    FROM company
                    WHERE LOWER(COALESCE(clutch_scrap, '')) != 'true'
                       OR LOWER(COALESCE(ambitionbox_scrap, '')) != 'true'
                       OR LOWER(COALESCE(goodfirms_scrap, '')) != 'true'
                """)
                rows = cur.fetchall()
                for company_id, company_name, reviews_urls, clutch_scrap, ambitionbox_scrap, goodfirms_scrap in rows:
                    if reviews_urls:
                        for source in reviews_urls:
                            src = source.get('source')
                            link = source.get('link')
                            # Use only DB columns for flag checks (as strings, case-insensitive)
                            if src and link:
                                if src == "clutch" and str(clutch_scrap).lower() != 'true':
                                    ClutchScraper().run(url=link, company_id=company_id, company_name=company_name)
                                elif src == "ambitionbox" and str(ambitionbox_scrap).lower() != 'true':
                                    AmbitionBoxScraper().run(url=link, company_id=company_id, company_name=company_name)
                                elif src == "goodfirms" and str(goodfirms_scrap).lower() != 'true':
                                    GoodFirmsScraper().run(url=link, company_id=company_id, company_name=company_name)
                                else:
                                    logger.info(f"Source '{src}' already scraped for company '{company_name}'")
        return jsonify({'message': 'Scraping triggered'}), 200
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

def fetch_company_reviews(company_slug, source):
    """Fetch reviews for a company and source."""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT company_id, company_name FROM company WHERE company_slug_name = %s",
                    (company_slug,)
                )
                company = cur.fetchone()
                if not company:
                    return jsonify({'error': 'Company not found'}), 404
                company_id, company_name = company

                cur.execute(
                    """SELECT company_review_id, designation, last_scraped, review_date, review_text, 
                              reviewer_name, source, source_url, user_rating
                       FROM company_reviews
                       WHERE company_id = %s AND source = %s""",
                    (company_id, source)
                )
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                reviews = [dict(zip(columns, row)) for row in rows]
                return jsonify({
                    'company_id': company_id,
                    'company_name': company_name,
                    'reviews': reviews
                })
    except Exception as e:
        logger.error(f"Error fetching reviews: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/clutch-reviews/<company_slug>', methods=['GET'])
def get_clutch_reviews(company_slug):
    return fetch_company_reviews(company_slug, 'clutch')

@app.route('/ambitionbox-reviews/<company_slug>', methods=['GET'])
def get_ambitionbox_reviews(company_slug):
    return fetch_company_reviews(company_slug, 'ambitionbox')

@app.route('/goodfirms-reviews/<company_slug>', methods=['GET'])
def get_goodfirms_reviews(company_slug):
    return fetch_company_reviews(company_slug, 'goodfirms')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
