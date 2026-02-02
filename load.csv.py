from dotenv import load_dotenv
load_dotenv()
import psycopg2
import csv
import os

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port="5432"
)

cur = conn.cursor()
print('Connected')
rows = []
with open("indigo.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        rows.append(row)
        print(row)
cur.execute("""
    INSERT INTO indigo
    (Airline, Date_of_Journey, Source, Destination, Route, Dep_Time,Duration, Total_Stops, Price)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", rows)

conn.commit()
cur.close()
conn.close()
print("CSV imported successfully (FAST)")
