import streamlit as st
from database import DB
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Create DB connection once
@st.cache_resource
def get_db():
    return DB()

db = get_db()

st.sidebar.title(' Flights Analytics')

user_option = st.sidebar.selectbox(
    'Menu',
    ['Select One', 'Check Flights', 'Analytics']
)

# ---------------- CHECK FLIGHTS ----------------
if user_option == 'Check Flights':
    st.title('Check Flight')

    col1, col2 = st.columns(2)
    city = db.fetch_city_name()

    with col1:
        source = st.selectbox('Source', sorted(city))
    with col2:
        destination = st.selectbox('Destination', sorted(city))

    if st.button('Search'):
        result = db.fetch_all_flight(source, destination)

        if result:
            df = pd.DataFrame(
            result,
            columns=["Airline", "Route", "Departure Time (HH:MM)", "Duration (mins)", "Price (₹)"]
                            )

            st.dataframe(df, width="stretch")
        else:
            st.warning("No flights found for this route.")

# ---------------- ANALYTICS ----------------
elif user_option == 'Analytics':
    st.title('Flight Analytics')

    # Pie chart
    airline, frequency = db.fetch_airline_frequency()
    fig = go.Figure(
        go.Pie(
            labels=airline,
            values=frequency,
            hoverinfo="label+percent",
            textinfo="value"
        )
    )
    st.subheader("Flights by Airline")
    st.plotly_chart(fig, width="stretch")

    # Busy airports bar chart
    city, frequency1 = db.fetch_busy_airport()
    fig = px.bar(
        x=city,
        y=frequency1,
        labels={'x': 'City', 'y': 'Number of Flights'}
    )
    st.subheader("Busiest Airports")
    st.plotly_chart(fig, width="stretch")

    # Daily frequency line chart
    date, frequency2 = db.daily_frequency()
    fig = px.line(
        x=date,
        y=frequency2,
        labels={'x': 'Date', 'y': 'Flights'}
    )
    st.subheader("Flights per Day")
    st.plotly_chart(fig, width="stretch")

# ---------------- HOME ----------------
else:
    st.title('About the Project')
    st.write("""
    This project analyzes flight data stored in **Azure SQL Database**.
    
    Features:
    - Search flights by source and destination
    - Airline-wise distribution
    - Busiest airports
    - Daily flight trends
    
    Built using:
    - Python
    - Pandas
    - Streamlit
    - PostgreSQL
    - Plotly
    """)
