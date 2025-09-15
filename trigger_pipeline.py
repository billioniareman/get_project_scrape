# pip install psycopg2-binary requests
import os, select, psycopg2, requests, logging, time, uuid, json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pg-listener")

DB_HOST = os.getenv("PGHOST", "ss-stag-dev-db-paij5iezee.supersourcing.com")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "getproject_staging")
DB_USER = os.getenv("PGUSER", "bluerangZbEbusr")
DB_PASS = os.getenv("PGPASSWORD", "Year#2015eba")
CHANNEL = os.getenv("PG_CHANNEL", "notify_company_reviews_ready")
FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = int(os.getenv("FLASK_PORT", "4200"))
API_URL = os.getenv("SCRAPE_URL", f"http://{FLASK_HOST}:{FLASK_PORT}/scrape")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))

def connect():
    while True:
        try:
            log.info("Connecting PG %s:%s/%s as %s", DB_HOST, DB_PORT, DB_NAME, DB_USER)
            conn = psycopg2.connect(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT)
            conn.set_session(autocommit=True)
            log.info("Connected.")
            return conn
        except Exception as e:
            log.error("DB connect failed: %s", e)
            time.sleep(3)

def check_flask():
    try:
        r = requests.get(API_URL.rsplit("/", 1)[0], timeout=HTTP_TIMEOUT)
        log.info("Flask health GET %s -> %s %s", API_URL.rsplit("/", 1)[0], r.status_code, r.text)
    except Exception as e:
        log.warning("Flask health failed: %s", e)

def listen_loop():
    conn = connect()
    cur = conn.cursor()
    cur.execute(f"LISTEN {CHANNEL};")
    log.info("LISTENing on channel %r …", CHANNEL)
    last_heartbeat = time.time()

    while True:
        try:
            ready = select.select([conn], [], [], 5)
            now = time.time()
            if ready == ([], [], []):
                if now - last_heartbeat >= 60:
                    last_heartbeat = now
                    log.info("Still listening… no notifications in the last 60s")
                continue

            conn.poll()
            while conn.notifies:
                n = conn.notifies.pop(0)
                req_id = str(uuid.uuid4())
                log.info("[req:%s] NOTIFY channel=%s pid=%s payload=%s", req_id, n.channel, n.pid, n.payload)

                try:
                    company_id = int(n.payload)
                except Exception:
                    company_id = None
                    log.warning("[req:%s] Non-int payload: %r", req_id, n.payload)

                # Optional: fetch the row for visibility
                try:
                    with conn.cursor() as c:
                        if company_id is not None:
                            c.execute("""
                                SELECT company_id, company_name, reviews_urls
                                FROM public.company
                                WHERE company_id = %s
                            """, (company_id,))
                            row = c.fetchone()
                            log.info("[req:%s] Row: %r", req_id, row)
                except Exception as e:
                    log.exception("[req:%s] Row fetch failed: %s", req_id, e)

                # POST to Flask
                try:
                    payload = {"company_id": company_id} if company_id is not None else {}
                    t0 = time.time()
                    r = requests.post(API_URL, json=payload, timeout=HTTP_TIMEOUT, headers={"X-Request-ID": req_id})
                    dt = (time.time() - t0) * 1000
                    log.info("[req:%s] POST %s body=%s -> %s in %.1fms resp=%s",
                             req_id, API_URL, payload, r.status_code, dt, r.text)
                    r.raise_for_status()
                except Exception as e:
                    log.exception("[req:%s] POST to Flask failed: %s", req_id, e)

        except psycopg2.OperationalError as e:
            log.error("DB OperationalError; reconnecting: %s", e)
            time.sleep(2)
            conn = connect()
            cur = conn.cursor()
            cur.execute(f"LISTEN {CHANNEL};")
            log.info("Re-LISTENing on %r …", CHANNEL)
        except Exception as e:
            log.exception("Unexpected error: %s", e)
            time.sleep(2)

if __name__ == "__main__":
    log.info("Starting listener FLASK=%s PG=%s:%s/%s channel=%s", API_URL, DB_HOST, DB_PORT, DB_NAME, CHANNEL)
    check_flask()
    listen_loop()
