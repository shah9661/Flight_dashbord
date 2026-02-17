from dotenv import load_dotenv
load_dotenv()

import psycopg2
import csv
import os
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port="5432"
)

cur = conn.cursor()
print(" Connected to DB")

rows = []
with open("indigo.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        rows.append(row)

query = """
INSERT INTO public.indigo
(airline, date_of_journey, source, destination, route, dep_time, duration, total_stops, price)
VALUES %s
"""

execute_values(cur, query, rows)

conn.commit()

cur.close()
conn.close()

