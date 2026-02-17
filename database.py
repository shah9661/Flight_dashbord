import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()


class DB:

    def __init__(self):
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=5432,
                sslmode="require",
                connect_timeout=30
            )

        except Exception as e:
            self.connection_pool = None

    # Utility Methods


    def get_connection(self):
        if self.connection_pool:
            return self.connection_pool.getconn()
        return None

    def return_connection(self, conn):
        if self.connection_pool and conn:
            self.connection_pool.putconn(conn)


    # Fetch City Names
   

    def fetch_city_name(self):
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT DISTINCT destination FROM public.indigo
                UNION
                SELECT DISTINCT source FROM public.indigo
            """)

            data = cursor.fetchall()
            cursor.close()

            return [row[0] for row in data]

        except Exception as e:
            return []

        finally:
            if conn:
                self.return_connection(conn)

    
    # Fetch Flights Between Cities
    

    def fetch_all_flight(self, source, destination):
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            query = """
                SELECT airline, route, dep_time, duration, price
                FROM public.indigo
                WHERE source = %s AND destination = %s
            """

            cursor.execute(query, (source, destination))
            data = cursor.fetchall()
            cursor.close()

            return data

        except Exception as e:
            return []

        finally:
            if conn:
                self.return_connection(conn)

    # Airline Frequency (Pie Chart)

    def fetch_airline_frequency(self):
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT airline, COUNT(*)
                FROM public.indigo
                GROUP BY airline
            """)

            data = cursor.fetchall()
            cursor.close()

            return [x[0] for x in data], [x[1] for x in data]

        except Exception as e:
            return [], []

        finally:
            if conn:
                self.return_connection(conn)
    # Busy Airport (Bar Chart)


    def fetch_busy_airport(self):
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT city, COUNT(*)
                FROM (
                    SELECT source AS city FROM public.indigo
                    UNION ALL
                    SELECT destination FROM public.indigo
                ) t
                GROUP BY city
                ORDER BY COUNT(*) DESC
            """)

            data = cursor.fetchall()
            cursor.close()

            return [x[0] for x in data], [x[1] for x in data]

        except Exception as e:
            return [], []

        finally:
            if conn:
                self.return_connection(conn)

    # Daily Frequency (Line Chart)
  

    def daily_frequency(self):
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT date_of_journey, COUNT(*)
                FROM public.indigo
                GROUP BY date_of_journey
                ORDER BY date_of_journey
            """)

            data = cursor.fetchall()
            cursor.close()

            return [x[0] for x in data], [x[1] for x in data]

        except Exception as e:
            return [], []

        finally:
            if conn:
                self.return_connection(conn)

    
    # Close Entire Pool 


    def close_pool(self):
        if self.connection_pool:
            self.connection_pool.closeall()

