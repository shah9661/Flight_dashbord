import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class DB:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=5432,
                sslmode="require",
                connect_timeout=30
            )
            self.cursor = self.conn.cursor()
            print(" PostgreSQL connection established")

        except Exception as e:
            print(" Connection error:", e)

    def fetch_city_name(self):
        try:
            self.cursor.execute("""
                SELECT DISTINCT destination FROM public.indigo
                UNION
                SELECT DISTINCT source FROM public.indigo
            """)
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            self.conn.rollback()
            print(" fetch_city_name error:", e)
            return []

    def fetch_all_flight(self, source, destination):
        try:
            query = """
                SELECT airline, route, dep_time, duration, price
                FROM public.indigo
                WHERE source = %s AND destination = %s
            """
            self.cursor.execute(query, (source, destination))
            return self.cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            print("fetch_all_flight error:", e)
            return []

    def fetch_airline_frequency(self):
        try:
            self.cursor.execute("""
                SELECT airline, COUNT(*)
                FROM public.indigo
                GROUP BY airline
            """)
            data = self.cursor.fetchall()
            return [x[0] for x in data], [x[1] for x in data]
        except Exception as e:
            self.conn.rollback()
            print(" fetch_airline_frequency error:", e)
            return [], []

    def fetch_busy_airport(self):
        try:
            self.cursor.execute("""
                SELECT city, COUNT(*)
                FROM (
                    SELECT source AS city FROM public.indigo
                    UNION ALL
                    SELECT destination FROM public.indigo
                ) t
                GROUP BY city
                ORDER BY COUNT(*) DESC
            """)
            data = self.cursor.fetchall()
            return [x[0] for x in data], [x[1] for x in data]
        except Exception as e:
            self.conn.rollback()
            print(" fetch_busy_airport error:", e)
            return [], []

    def daily_frequency(self):
        try:
            self.cursor.execute("""
                SELECT date_of_journey, COUNT(*)
                FROM public.indigo
                GROUP BY date_of_journey
                ORDER BY date_of_journey
            """)
            data = self.cursor.fetchall()
            return [x[0] for x in data], [x[1] for x in data]
        except Exception as e:
            self.conn.rollback()
            print(" daily_frequency error:", e)
            return [], []

    def close(self):
        self.cursor.close()
        self.conn.close()

        