import os
import pymysql





import logging
import time

log = logging.getLogger(__name__)


def get_conn(retries=12, delay=5):
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(
                host=os.getenv("MYSQL_HOST", "localhost"),
                port=int(os.getenv("MYSQL_PORT", "3307")),
                user=os.environ["MYSQL_USER"],
                password=os.environ["MYSQL_PASSWORD"],
                database=os.environ["MYSQL_DATABASE"],
                autocommit=False,
                connect_timeout=10,
            )
        except pymysql.err.OperationalError as exc:
            if attempt == retries:
                raise
            log.warning("Base pas prête (%s). Essai %s/%s, nouvel essai dans %ss",
                        exc, attempt, retries, delay)
            time.sleep(delay)


def get_source(conn, source_id):
    with conn.cursor() as cur:
        cur.execute("SELECT name, base_url FROM dim_source WHERE source_id=%s", (source_id,))
        return cur.fetchone()


def get_external_ids(conn, source_id):
    with conn.cursor() as cur:
        cur.execute("SELECT external_id FROM product_source_map WHERE source_id=%s", (source_id,))
        return [row[0] for row in cur.fetchall()]


def load_staging(conn, rows, source_id, collected_at):
    try:
        with conn.cursor() as cur:
            # DELETE et non TRUNCATE : TRUNCATE valide la transaction en MySQL (DDL)
            cur.execute(
                "DELETE FROM stg_price WHERE source_id=%s AND collected_at=%s",
                (source_id, collected_at),
            )
            cur.execute(
                "SELECT external_id, product_id FROM product_source_map WHERE source_id=%s",
                (source_id,),
            )
            mapping = dict(cur.fetchall())

            data = []
            for r in rows:
                product_id = mapping.get(r["external_id"])
                if product_id is None:
                    raise ValueError(f"Produit externe inconnu : {r['external_id']}")
                data.append((product_id, source_id, collected_at,
                             r["list_price"], r["discount_pct"], r["price"], r["currency"]))

            cur.executemany(
                """INSERT INTO stg_price
                   (product_id, source_id, collected_at, list_price, discount_pct, price, currency)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                data,
            )
        conn.commit()
        return len(data)
    except Exception:
        conn.rollback()
        raise


def upsert_final(conn, source_id, collected_at):
    """Copie le staging vers fact_price en une seule transaction."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO fact_price
                   (product_id, source_id, collected_at, list_price, discount_pct, price, currency)
                   SELECT product_id, source_id, collected_at, list_price, discount_pct, price, currency
                   FROM stg_price
                   WHERE source_id=%s AND collected_at=%s
                   ON DUPLICATE KEY UPDATE
                     list_price   = VALUES(list_price),
                     discount_pct = VALUES(discount_pct),
                     price        = VALUES(price),
                     currency     = VALUES(currency)""",
                (source_id, collected_at),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise