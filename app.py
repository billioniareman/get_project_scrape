from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
import sys
import asyncio
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import logging
import atexit

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

# Flask app setup
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL credentials
PG_HOST = "ss-stag-dev-db-paij5iezee.supersourcing.com"
PG_PORT = 5432
PG_DBNAME = "bluerang_test_master_db"
PG_USER = "bluerangZbEbusr"
PG_PASSWORD = "Year#2015eba"

# PostgreSQL connection pool
db_pool = ThreadedConnectionPool(
    minconn=2,
    maxconn=10,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DBNAME
)

# Close DB pool on shutdown
@atexit.register
def close_db_pool():
    if db_pool:
        db_pool.closeall()
        logger.info("Database pool closed.")

# Helper to get a DB connection
def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
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
                # Fetch companies where at least one platform is not scraped
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
        db_pool.putconn(conn)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
