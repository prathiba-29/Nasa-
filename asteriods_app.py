# In[1]:

import streamlit as st
import pandas as pd
import sqlite3

# Title
st.set_page_config(page_title="NASA NEO Dashboard", layout="wide")
st.title("🚀 NASA Near-Earth Object (NEO) Dashboard")

# Connect to DB
@st.cache_resource
def get_connection():
    return sqlite3.connect("nasa_neo.db")

conn = get_connection()

# Predefined Queries
queries = {
    "Count how many times each asteroid has approached Earth":
        """SELECT a.name, COUNT(*) AS approach_count
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           GROUP BY a.name
           ORDER BY approach_count DESC;""",

    "Average velocity of each asteroid":
        """SELECT a.name, AVG(c.relative_velocity_kmph) AS avg_velocity
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           GROUP BY a.name
           ORDER BY avg_velocity DESC;""",

    "Top 10 fastest asteroids":
        """SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           GROUP BY a.name
           ORDER BY max_velocity DESC
           LIMIT 10;""",

    "Hazardous asteroids with >3 approaches":
        """SELECT a.name, COUNT(*) AS approaches
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           WHERE a.is_potentially_hazardous_asteroid = 1
           GROUP BY a.name
           HAVING approaches > 3
           ORDER BY approaches DESC;""",

    "Month with the most approaches":
        """SELECT strftime('%Y-%m', close_approach_date) AS month, COUNT(*) AS total_approaches
           FROM close_approach
           GROUP BY month
           ORDER BY total_approaches DESC
           LIMIT 1;""",

    "Fastest asteroid ever recorded":
        """SELECT a.name, c.relative_velocity_kmph, c.close_approach_date
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           ORDER BY c.relative_velocity_kmph DESC
           LIMIT 1;""",

    "Largest asteroids by max diameter":
        """SELECT name, estimated_diameter_max_km
           FROM asteroids
           ORDER BY estimated_diameter_max_km DESC;""",

    "Closest approaches in km (all asteroids)":
        """SELECT a.name, c.close_approach_date, c.miss_distance_km
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           ORDER BY c.miss_distance_km ASC;""",

    "Closest approach per asteroid":
        """SELECT name, MIN(miss_distance_km) AS closest_distance
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           GROUP BY name;""",

    "Asteroids moving faster than 50,000 km/h":
        """SELECT DISTINCT a.name, c.relative_velocity_kmph
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           WHERE c.relative_velocity_kmph > 50000
           ORDER BY c.relative_velocity_kmph DESC;""",

    "Monthly approaches count":
        """SELECT strftime('%Y-%m', close_approach_date) AS month, COUNT(*) AS total_approaches
           FROM close_approach
           GROUP BY month
           ORDER BY month ASC;""",

    "Brightest asteroid":
        """SELECT name, MIN(absolute_magnitude_h) AS brightest
           FROM asteroids;""",

    "Hazardous vs Non-Hazardous count":
        """SELECT CASE WHEN is_potentially_hazardous_asteroid = 1 THEN 'Hazardous'
                       ELSE 'Non-Hazardous' END AS hazard_status,
                  COUNT(*) AS count
           FROM asteroids
           GROUP BY is_potentially_hazardous_asteroid;""",

    "Asteroids within 1 lunar distance":
        """SELECT a.name, c.close_approach_date, c.miss_distance_lunar
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           WHERE c.miss_distance_lunar < 1
           ORDER BY c.miss_distance_lunar ASC;""",

    "Asteroids within 0.05 AU":
        """SELECT a.name, c.close_approach_date, c.astronomical
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           WHERE c.astronomical < 0.05
           ORDER BY c.astronomical ASC;""",

    "Asteroid with longest name":
        """SELECT name, LENGTH(name) AS name_length
           FROM asteroids
           ORDER BY name_length DESC
           LIMIT 1;""",

    "Most common orbiting body":
        """SELECT orbiting_body, COUNT(*) AS total_approaches
           FROM close_approach
           GROUP BY orbiting_body
           ORDER BY total_approaches DESC;""",

    "Average miss distance by hazard status":
        """SELECT CASE WHEN a.is_potentially_hazardous_asteroid = 1 THEN 'Hazardous'
                       ELSE 'Non-Hazardous' END AS hazard_status,
                  AVG(c.miss_distance_km) AS avg_miss_distance_km
           FROM asteroids a
           JOIN close_approach c ON a.id = c.neo_reference_id
           GROUP BY hazard_status;""",

    "Smallest asteroids by min diameter":
        """SELECT name, estimated_diameter_min_km
           FROM asteroids
           ORDER BY estimated_diameter_min_km ASC
           LIMIT 5;""",

    "Average monthly velocity":
        """SELECT strftime('%Y-%m', c.close_approach_date) AS month,
                  AVG(c.relative_velocity_kmph) AS avg_monthly_velocity
           FROM close_approach c
           GROUP BY month
           ORDER BY month;"""
}

# %%
