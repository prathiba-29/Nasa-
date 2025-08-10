#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import datetime
import sqlite3


# In[8]:


import requests
import pandas as pd

API_KEY = "lEudM1s6IUHwefLsPiagzBNgS4lxAJpE13Favncq"
BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"

def fetch_neo_data(start_date, max_records=10000):
    all_data = []
    url = f"{BASE_URL}?start_date={start_date}&api_key={API_KEY}"
    
    while url and len(all_data) < max_records:
        print(f"Fetching data from: {url}")
        response = requests.get(url)
        
        if response.status_code != 200:
            print("Error:", response.status_code)
            break
        
        data = response.json()

        for date, asteroids in data['near_earth_objects'].items():
            for asteroid in asteroids:
                try:
                    approach = asteroid['close_approach_data'][0]
                    record = {
                        "id": int(asteroid["id"]),
                        "neo_reference_id": int(asteroid["neo_reference_id"]),
                        "name": asteroid["name"],
                        "absolute_magnitude_h": float(asteroid["absolute_magnitude_h"]),
                        "estimated_diameter_min_km": float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"]),
                        "estimated_diameter_max_km": float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"]),
                        "is_potentially_hazardous_asteroid": asteroid["is_potentially_hazardous_asteroid"],
                        "close_approach_date": approach["close_approach_date"],
                        "relative_velocity_kmph": float(approach["relative_velocity"]["kilometers_per_hour"]),
                        "astronomical": float(approach["miss_distance"]["astronomical"]),
                        "miss_distance_km": float(approach["miss_distance"]["kilometers"]),
                        "miss_distance_lunar": float(approach["miss_distance"]["lunar"]),
                        "orbiting_body": approach["orbiting_body"]
                    }
                    all_data.append(record)
                except Exception as e:
                    print("Skipping a record due to:", e)

        url = data['links'].get('next')  # automatic 7-day pagination

    return pd.DataFrame(all_data)

# ✅ Run it starting from 2025-01-07, it will auto-paginate every 7 days
df = fetch_neo_data("2025-01-07", max_records=1000)
df.head()


# In[9]:


import sqlite3

# Connect to (or create) the SQLite DB
conn = sqlite3.connect("neo_database.db")
cursor = conn.cursor()


# In[10]:


# Asteroid Info Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS asteroids (
    id INTEGER,
    name TEXT,
    absolute_magnitude_h REAL,
    estimated_diameter_min_km REAL,
    estimated_diameter_max_km REAL,
    is_potentially_hazardous_asteroid BOOLEAN
)
""")

# Close Approach Info Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS close_approach (
    neo_reference_id INTEGER,
    close_approach_date TEXT,
    relative_velocity_kmph REAL,
    astronomical REAL,
    miss_distance_km REAL,
    miss_distance_lunar REAL,
    orbiting_body TEXT
)
""")
conn.commit()


# In[11]:


for _, row in df.iterrows():
    try:
        # Insert into asteroids table
        cursor.execute("""
        INSERT INTO asteroids VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row['id'],
            row['name'],
            row['absolute_magnitude_h'],
            row['estimated_diameter_min_km'],
            row['estimated_diameter_max_km'],
            row['is_potentially_hazardous_asteroid']
        ))

        # Insert into close_approach table
        cursor.execute("""
        INSERT INTO close_approach VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['neo_reference_id'],
            row['close_approach_date'],
            row['relative_velocity_kmph'],
            row['astronomical'],
            row['miss_distance_km'],
            row['miss_distance_lunar'],
            row['orbiting_body']
        ))
    except Exception as e:
        print("Insert error:", e)

conn.commit()
conn.close()
print("✅ Data inserted successfully into SQLite!")


# In[18]:


import pandas as pd
import sqlite3
conn = sqlite3.connect("neo_database.db")


# In[19]:


q1 = """
SELECT a.name, COUNT(*) AS approach_count
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
GROUP BY a.name
ORDER BY approach_count DESC;
"""
pd.read_sql(q1, conn)


# In[20]:


q2 = """
SELECT a.name, AVG(c.relative_velocity_kmph) AS avg_velocity
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
GROUP BY a.name
ORDER BY avg_velocity DESC;
"""
pd.read_sql(q2, conn)


# In[21]:


q3 = """
SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
GROUP BY a.name
ORDER BY max_velocity DESC
LIMIT 10;
"""
pd.read_sql(q3, conn)


# In[22]:


q4 = """
SELECT a.name, COUNT(*) AS approaches
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
WHERE a.is_potentially_hazardous_asteroid = 1
GROUP BY a.name
HAVING approaches > 3
ORDER BY approaches DESC;
"""
pd.read_sql(q4, conn)


# In[24]:


q5 = """
SELECT strftime('%Y-%m', close_approach_date) AS month, COUNT(*) AS total_approaches
FROM close_approach
GROUP BY month
ORDER BY total_approaches DESC
LIMIT 1;
"""
pd.read_sql(q5, conn)


# In[25]:


q6 = """
SELECT a.name, c.relative_velocity_kmph, c.close_approach_date
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
ORDER BY c.relative_velocity_kmph DESC
LIMIT 1;
"""
pd.read_sql(q6, conn)


# In[26]:


q7 = """
SELECT name, estimated_diameter_max_km
FROM asteroids
ORDER BY estimated_diameter_max_km DESC;
"""
pd.read_sql(q7, conn)


# In[27]:


q8 = """
SELECT a.name, c.close_approach_date, c.miss_distance_km
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
ORDER BY a.name, c.close_approach_date, c.miss_distance_km ASC;
"""
pd.read_sql(q8, conn)


# In[28]:


q9 = """
SELECT name, close_approach_date, MIN(miss_distance_km) AS closest_distance
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
GROUP BY name;
"""
pd.read_sql(q9, conn)


# In[29]:


q10 = """
SELECT DISTINCT a.name, c.relative_velocity_kmph
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
WHERE c.relative_velocity_kmph > 50000
ORDER BY c.relative_velocity_kmph DESC;
"""
pd.read_sql(q10, conn)


# In[30]:


q11 = """
SELECT strftime('%Y-%m', close_approach_date) AS month, COUNT(*) AS total_approaches
FROM close_approach
GROUP BY month
ORDER BY month ASC;
"""
pd.read_sql(q11, conn)


# In[31]:


q12 = """
SELECT name, MIN(absolute_magnitude_h) AS brightest
FROM asteroids;
"""
pd.read_sql(q12, conn)


# In[32]:


q13 = """
SELECT 
    CASE WHEN is_potentially_hazardous_asteroid = 1 THEN 'Hazardous' ELSE 'Non-Hazardous' END AS hazard_status,
    COUNT(*) AS count
FROM asteroids
GROUP BY is_potentially_hazardous_asteroid;
"""
pd.read_sql(q13, conn)


# In[33]:


q14 = """
SELECT a.name, c.close_approach_date, c.miss_distance_lunar
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
WHERE c.miss_distance_lunar < 1
ORDER BY c.miss_distance_lunar ASC;
"""
pd.read_sql(q14, conn)


# In[34]:


q15 = """
SELECT a.name, c.close_approach_date, c.astronomical
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
WHERE c.astronomical < 0.05
ORDER BY c.astronomical ASC;
"""
pd.read_sql(q15, conn)


# In[35]:


q16 = """
SELECT name, LENGTH(name) AS name_length
FROM asteroids
ORDER BY name_length DESC
LIMIT 1;
"""
pd.read_sql(q16, conn)


# In[36]:


q17 = """
SELECT orbiting_body, COUNT(*) AS total_approaches
FROM close_approach
GROUP BY orbiting_body
ORDER BY total_approaches DESC;
"""
pd.read_sql(q17, conn)


# In[37]:


q18 = """
SELECT 
    CASE WHEN a.is_potentially_hazardous_asteroid = 1 THEN 'Hazardous' ELSE 'Non-Hazardous' END AS hazard_status,
    AVG(c.miss_distance_km) AS avg_miss_distance_km
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
GROUP BY hazard_status;
"""
pd.read_sql(q18, conn)


# In[38]:


q19 = """
SELECT name, estimated_diameter_min_km
FROM asteroids
ORDER BY estimated_diameter_min_km ASC
LIMIT 5;
"""
pd.read_sql(q19, conn)


# In[39]:


q20 = """
SELECT strftime('%Y-%m', c.close_approach_date) AS month,
       AVG(c.relative_velocity_kmph) AS avg_monthly_velocity
FROM close_approach c
GROUP BY month
ORDER BY month;
"""
pd.read_sql(q20, conn)



# %%




# %%
