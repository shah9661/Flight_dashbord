import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

class DB:
    def __init__(self):
        try:
           self.conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;'
            )

           self.cursor = self.conn.cursor()
           print("Azure SQL connection established")
        except Exception as e:
            print("Connection error:", e)

    def fetch_city_name(self):
        city = []
        self.cursor.execute("""
            SELECT DISTINCT Destination FROM indigo
            UNION
            SELECT DISTINCT Source FROM indigo
        """)
        data = self.cursor.fetchall()
        for item in data:
            city.append(item[0])
        return city

    def fetch_all_flight(self, source, destination):
        query = """
        SELECT 
            Airline,
            Route,
            Dep_Time,
            Duration,
            Price
        FROM indigo
        WHERE Source = ? AND Destination = ?
    """
        self.cursor.execute(query, (source, destination))
        rows = self.cursor.fetchall()
        return [tuple(row) for row in rows]


    def fetch_airline_frequency(self):
        airline = []
        frequency = []
        self.cursor.execute("""
            SELECT Airline, COUNT(*) 
            FROM indigo 
            GROUP BY Airline
        """)
        data = self.cursor.fetchall()
        for item in data:
            airline.append(item[0])
            frequency.append(item[1])
        return airline, frequency

    def fetch_busy_airport(self):
        city = []
        frequency = []
        self.cursor.execute("""
            SELECT Source, COUNT(*) 
            FROM (
                SELECT Source FROM indigo
                UNION ALL
                SELECT Destination FROM indigo
            ) t
            GROUP BY Source
            ORDER BY COUNT(*) DESC
        """)
        data = self.cursor.fetchall()
        for item in data:
            city.append(item[0])
            frequency.append(item[1])
        return city, frequency

    def daily_frequency(self):
        date = []
        frequency = []
        self.cursor.execute("""
            SELECT Date_of_Journey, COUNT(*) 
            FROM indigo
            GROUP BY Date_of_Journey
        """)
        data = self.cursor.fetchall()
        for item in data:
            date.append(item[0])
            frequency.append(item[1])
        return sorted(date), frequency

    def close(self):
        self.conn.close()
