from flask import Flask, jsonify, request
import requests
import logging
import time
import uuid
import os
import json
import platform
from Ambition_scrape import AmbitionBoxScraper
from Clutch_scrape import ClutchScraper
from Goodfirm_scrape import GoodFirmsScraper

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("scrape-server")

# -------------------------
# Config
# -------------------------
NODE_HOST = os.getenv("NODE_HOST", "localhost")
NODE_PORT = int(os.getenv("NODE_PORT", "3000"))
NODE_BASE = f"http://{NODE_HOST}:{NODE_PORT}"

GET_COMPANY_API = f"{NODE_BASE}/api/v2/getCompanyDataForReviewScrap"
SAVE_REVIEWS_API = f"{NODE_BASE}/api/v2/save-company-reviews"

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

# Optional: pin UC driver major version via env if needed (e.g., 139)
UC_VERSION_MAIN = os.getenv("UC_VERSION_MAIN")
# Optional: run Selenium headless (default True in servers)
UC_HEADLESS = os.getenv("UC_HEADLESS", "true").lower() in ("1", "true", "yes")

app = Flask(__name__)

# -------------------------
# Utils
# -------------------------
def _shorten(obj, limit: int = 500) -> str:
    if isinstance(obj, (dict, list)):
        try:
            s = json.dumps(obj)
        except Exception:
            s = str(obj)
    else:
        s = str(obj)
    return s[:limit] + ("... (truncated)" if len(s) > limit else "")

def _timed_request(method, url, **kwargs):
    t0 = time.time()
    r = method(url, timeout=HTTP_TIMEOUT, **kwargs)
    dt = (time.time() - t0) * 1000
    return r, dt

def send_reviews_to_api(data, request_id: str):
    try:
        r, dt = _timed_request(requests.post, SAVE_REVIEWS_API, json=data)
        logger.info("[req:%s] POST %s -> %s in %.1fms body=%s",
                    request_id, SAVE_REVIEWS_API, r.status_code, dt, _shorten(r.text))
        r.raise_for_status()
        return True
    except Exception as e:
        logger.exception("[req:%s] Failed POST %s: %s", request_id, SAVE_REVIEWS_API, e)
        return False

def fetch_companies(request_id: str):
    r, dt = _timed_request(requests.get, GET_COMPANY_API)
    logger.info("[req:%s] GET %s -> %s in %.1fms body=%s",
                request_id, GET_COMPANY_API, r.status_code, dt, _shorten(r.text))
    r.raise_for_status()
    payload = r.json()
    companies = payload.get("data")
    if companies is None:
        raise ValueError("Missing 'data' in Node response")
    if isinstance(companies, dict):
        companies = [companies]
    elif not isinstance(companies, list):
        raise TypeError(f"'data' must be list|dict, got {type(companies)}")
    logger.info("[req:%s] Companies fetched: %d", request_id, len(companies))
    return companies

# -------------------------
# Health / diagnostics
# -------------------------
@app.get("/")
def health():
    return "OK"

@app.get("/health/ready")
def ready():
    try:
        r, dt = _timed_request(requests.get, GET_COMPANY_API)
        return jsonify({"status": "ready", "node_status": r.status_code, "latency_ms": dt}), 200
    except Exception as e:
        return jsonify({"status": "degraded", "error": str(e)}), 502

@app.get("/diag/selenium")
def diag_selenium():
    """
    Lightweight driver smoke test.
    Returns Chrome version, driver attempt, and any exception message.
    Does NOT scrape sites.
    """
    info = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "UC_VERSION_MAIN": UC_VERSION_MAIN,
        "UC_HEADLESS": UC_HEADLESS,
    }
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.chrome.options import Options

        options = uc.ChromeOptions()
        if UC_HEADLESS:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        kwargs = {"options": options}
        if UC_VERSION_MAIN:
            kwargs["version_main"] = int(UC_VERSION_MAIN)

        t0 = time.time()
        driver = uc.Chrome(**kwargs)
        dt = (time.time() - t0) * 1000
        info["driver_init_ms"] = dt
        info["browser_version"] = driver.capabilities.get("browserVersion")
        info["chrome_version"] = driver.capabilities.get("chromeVersion")
        info["driver_version"] = driver.capabilities.get("chromedriverVersion") if "chrome" in driver.capabilities else "unknown"
        driver.quit()
        return jsonify({"ok": True, "info": info}), 200
    except Exception as e:
        logger.exception("Selenium diag failed")
        info["error"] = str(e)
        return jsonify({"ok": False, "info": info}), 500

# -------------------------
# Main scrape
# -------------------------
@app.post("/scrape")
def scrape():
    """
    Triggers scraping for all companies/sources.
    Accepts optional JSON: { "company_id": <int> } to filter.
    """
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    body = request.get_json(silent=True) or {}
    logger.info("[req:%s] /scrape called from %s body=%s", request_id, request.remote_addr, body)

    try:
        companies = fetch_companies(request_id)

        # Optional filter by company_id
        target_id = body.get("company_id")
        if target_id is not None:
            before = len(companies)
            companies = [c for c in companies if c.get("company_id") == target_id]
            logger.info("[req:%s] Filter company_id=%s -> %d/%d", request_id, target_id, len(companies), before)
            if not companies:
                return jsonify({"message": f"No company {target_id} from API"}), 404

        scraper_map = {
            "clutch": (ClutchScraper, "clutch_scrap"),
            "ambitionbox": (AmbitionBoxScraper, "ambitionbox_scrap"),
            "goodfirms": (GoodFirmsScraper, "goodfirms_scrap"),
        }

        totals = {"companies": len(companies), "sources": 0, "kicked": 0, "sent": 0, "errors": []}

        for company in companies:
            company_id = company.get("company_id")
            company_name = company.get("company_name")
            reviews_urls = (company.get("reviews_urls") or [])
            logger.info("[req:%s] Company id=%s name=%s sources=%d node_flags={clutch:%s, ambitionbox:%s, goodfirms:%s}",
                        request_id, company_id, company_name, len(reviews_urls),
                        company.get("clutch_scrap"), company.get("ambitionbox_scrap"), company.get("goodfirms_scrap"))

            for source in reviews_urls:
                totals["sources"] += 1
                src = (source or {}).get("source")
                link = (source or {}).get("link")
                if not src or not link:
                    logger.warning("[req:%s] Skipping invalid source entry: %s", request_id, source)
                    continue
                if src not in scraper_map:
                    logger.info("[req:%s] Unsupported source: %s", request_id, src)
                    continue

                scraper_cls, scrap_flag = scraper_map[src]
                already = str(company.get(scrap_flag)).lower() == "true"
                logger.info("[req:%s] Source=%s link=%s flag=%s already=%s", request_id, src, link, scrap_flag, already)
                if already:
                    continue

                totals["kicked"] += 1

                # Run scraper with hard exception boundary so one source doesn't kill the request
                try:
                    t0 = time.time()
                    scraper = scraper_cls()
                    logger.info("[req:%s] Running %s.run(company_id=%s, name=%s)", request_id, scraper_cls.__name__, company_id, company_name)
                    scraped_data = scraper.run(url=link, company_id=company_id, company_name=company_name)
                    dt = (time.time() - t0) * 1000
                    logger.info("[req:%s] %s done in %.1fms; data? %s ; sample=%s",
                                request_id, scraper_cls.__name__, dt, bool(scraped_data), _shorten(scraped_data, 400))
                except Exception as e:
                    # This is where your Chrome/driver mismatch was throwing
                    msg = f"{scraper_cls.__name__} failed for {company_name}({company_id})/{src}: {e}"
                    logger.exception("[req:%s] %s", request_id, msg)
                    totals["errors"].append(msg)
                    continue

                if scraped_data:
                    if send_reviews_to_api(scraped_data, request_id):
                        totals["sent"] += 1
                else:
                    logger.warning("[req:%s] No scraped_data for %s/%s", request_id, company_name, src)

        logger.info("[req:%s] DONE %s", request_id, totals)
        return jsonify({"message": "Scraping triggered", **totals}), 200

    except requests.exceptions.RequestException as e:
        logger.exception("[req:%s] Network error", request_id)
        return jsonify({"error": f"Network error: {e}"}), 502
    except Exception as e:
        logger.exception("[req:%s] Unhandled error", request_id)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "4200"))
    logger.info("Starting Flask on %s:%s NODE_BASE=%s", host, port, NODE_BASE)
    app.run(host=host, port=port)
