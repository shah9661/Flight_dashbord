from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port="5432"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS public.indigo (
    airline TEXT,
    date_of_journey DATE,
    source TEXT,
    destination TEXT,
    route TEXT,
    dep_time TIME,
    duration INTEGER,
    total_stops TEXT,
    price INTEGER
);
""")

conn.commit()  

cur.close()
conn.close()