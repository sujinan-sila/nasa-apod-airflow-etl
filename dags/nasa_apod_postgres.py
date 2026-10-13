import html
import re
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

POSTGRES_CONN_ID = "apod_db"
SCIENCE_CONN_ID = "nasa_science"
HEADERS = {"User-Agent": "nasa-apod-airflow-etl (github.com/sujinan-sila)"}


def _text(fragment: str) -> str:
    """HTML fragment -> plain text on one line."""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))).strip()


@dag(
    dag_id="nasa_apod_postgres",
    start_date=datetime(2026, 9, 1),
    # 14:00 UTC: APOD publishes on US Eastern time, so at midnight UTC
    # the current day's picture does not exist yet
    schedule="0 14 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "nasa"],
)
def nasa_apod_postgres():

    @task
    def create_table():
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run("""
            CREATE TABLE IF NOT EXISTS apod_data (
                apod_date   DATE PRIMARY KEY,
                title       TEXT NOT NULL,
                explanation TEXT,
                url         TEXT,
                media_type  VARCHAR(20),
                loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            ALTER TABLE apod_data ADD COLUMN IF NOT EXISTS page_url TEXT;
        """)

    @task
    def extract(ds=None) -> dict:
        day = datetime.strptime(ds, "%Y-%m-%d")
        prefix = f"APOD: {day.year} {day.strftime('%B')} {day.day} "   # "APOD: 2026 October 12 "
        hook = HttpHook(method="GET", http_conn_id=SCIENCE_CONN_ID)

        articles = hook.run(
            "wp-json/wp/v2/image-article",
            data={
                "search": prefix.strip(),
                "per_page": 10,
                "_fields": "id,title,link,content,excerpt,featured_media",
            },
            headers=HEADERS,
        ).json()
        matches = [a for a in articles if html.unescape(a["title"]["rendered"]).startswith(prefix)]
        if not matches:
            # Not published yet, or not migrated yet: a normal failure, so Airflow retries
            raise ValueError(f"no APOD article on science.nasa.gov for {ds}")
        article = matches[0]

        image_url = None
        if article.get("featured_media"):
            media = hook.run(
                f"wp-json/wp/v2/media/{article['featured_media']}",
                data={"_fields": "source_url"},
                headers=HEADERS,
            ).json()
            image_url = media.get("source_url")

        return {
            "date": ds,
            "title": article["title"]["rendered"],
            "page_url": article["link"],
            "content": article["content"]["rendered"],
            "excerpt": article["excerpt"]["rendered"],
            "image_url": image_url,
        }

    @task
    def transform(raw: dict) -> dict:
        # "APOD: 2026 October 12 – Jupiter and the Moons" -> "Jupiter and the Moons"
        title = re.sub(r"^APOD:\s*\d{4}\s+\w+\s+\d{1,2}\s*[–-]\s*", "", html.unescape(raw["title"])).strip()

        text = _text(raw["content"])
        found = re.search(r"Explanation:\s*(.*?)\s*Tomorrow[’']s picture", text)
        explanation = found.group(1) if found else re.sub(r"^Explanation:\s*", "", _text(raw["excerpt"]))

        url = raw["image_url"] or raw["page_url"]
        if not title or "nasa-logo" in url:
            raise AirflowFailException(f"article for {raw['date']} has no usable title or image")

        return {
            "apod_date": raw["date"],
            "title": title,
            "explanation": explanation,
            "url": url,
            "media_type": "image" if raw["image_url"] else "other",
            "page_url": raw["page_url"],
        }

    @task
    def load(row: dict):
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(
            """
            INSERT INTO apod_data (apod_date, title, explanation, url, media_type, page_url)
            VALUES (%(apod_date)s, %(title)s, %(explanation)s, %(url)s, %(media_type)s, %(page_url)s)
            ON CONFLICT (apod_date) DO UPDATE SET
                title       = EXCLUDED.title,
                explanation = EXCLUDED.explanation,
                url         = EXCLUDED.url,
                media_type  = EXCLUDED.media_type,
                page_url    = EXCLUDED.page_url,
                loaded_at   = now();
            """,
            parameters=row,
        )

    @task
    def check(ds=None):
        count, logos = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_first(
            """
            SELECT count(*), count(*) FILTER (WHERE url LIKE '%%nasa-logo%%')
            FROM apod_data WHERE apod_date = %s
            """,
            parameters=(ds,),
        )
        if count != 1 or logos:
            raise ValueError(f"{ds}: expected 1 real row, found {count} rows ({logos} with the logo)")
        print(f"{ds}: 1 row")

    raw = extract()
    create_table() >> raw
    load(transform(raw)) >> check()


nasa_apod_postgres()
