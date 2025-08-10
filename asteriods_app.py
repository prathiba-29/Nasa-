# In[1]:

"""
NASA NEO Streamlit App (full)
Place this file in the same folder as nasa_neo.db before deploying.
"""

import streamlit as st
import pandas as pd
import sqlite3
from io import BytesIO

st.set_page_config(page_title="🚀 NASA NEO Dashboard", layout="wide")

# -------------------------
# Helpers / DB connection
# -------------------------
@st.cache_resource
def get_conn(db_path: str = "nasa_neo.db"):
    """Return a sqlite3 connection (cached)."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    # enable row factory if needed
    conn.row_factory = sqlite3.Row
    return conn

def run_sql(sql: str, conn) -> pd.DataFrame:
    """Run sql and return DataFrame (caches can be added if needed)."""
    try:
        df = pd.read_sql_query(sql, conn, parse_dates=["close_approach_date"] if "close_approach_date" in sql else None)
    except Exception:
        # fallback without parse_dates
        df = pd.read_sql_query(sql, conn)
    return df

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    b = BytesIO()
    df.to_csv(b, index=False)
    return b.getvalue()

# -------------------------
# Queries (20 predefined)
# -------------------------
queries = {
    "1 Count how many times each asteroid has approached Earth":
        """
        SELECT a.name, COUNT(*) AS approach_count
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY approach_count DESC
        """,

    "2 Average velocity of each asteroid over multiple approaches":
        """
        SELECT a.name, ROUND(AVG(c.relative_velocity_kmph),2) AS avg_velocity_kmph
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY avg_velocity_kmph DESC
        """,

    "3 Top 10 fastest asteroids":
        """
        SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity_kmph
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY max_velocity_kmph DESC
        LIMIT 10
        """,

    "4 Potentially hazardous asteroids that approached more than 3 times":
        """
        SELECT a.name, COUNT(*) AS approaches
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE a.is_potentially_hazardous_asteroid = 1
        GROUP BY a.name
        HAVING COUNT(*) > 3
        ORDER BY approaches DESC
        """,

    "5 Month with the most asteroid approaches":
        """
        SELECT strftime('%Y-%m', c.close_approach_date) AS month, COUNT(*) AS approaches
        FROM close_approach c
        GROUP BY month
        ORDER BY approaches DESC
        LIMIT 1
        """,

    "6 Asteroid with the fastest ever approach speed":
        """
        SELECT a.name, c.relative_velocity_kmph
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        ORDER BY c.relative_velocity_kmph DESC
        LIMIT 1
        """,

    "7 Sort asteroids by maximum estimated diameter (descending)":
        """
        SELECT a.name, MAX(a.estimated_diameter_max_km) AS max_diameter_km
        FROM asteroids a
        GROUP BY a.name
        ORDER BY max_diameter_km DESC
        """,

    "8 Asteroid approach records ordered by date (show trends)":
        """
        SELECT a.name, c.close_approach_date, c.miss_distance_km
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        ORDER BY a.name, c.close_approach_date ASC
        """,

    "9 Closest approach date & miss distance for each asteroid":
        """
        SELECT a.name, MIN(c.miss_distance_km) AS closest_km
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY closest_km ASC
        """,

    "10 Asteroids with velocity > 50,000 km/h":
        """
        SELECT DISTINCT a.name, c.relative_velocity_kmph
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.relative_velocity_kmph > 50000
        ORDER BY c.relative_velocity_kmph DESC
        """,

    "11 Count of approaches per month":
        """
        SELECT strftime('%Y-%m', c.close_approach_date) AS month, COUNT(*) AS approach_count
        FROM close_approach c
        GROUP BY month
        ORDER BY month
        """,

    "12 Asteroid with the highest brightness (lowest magnitude)":
        """
        SELECT name, absolute_magnitude_h
        FROM asteroids
        ORDER BY absolute_magnitude_h ASC
        LIMIT 1
        """,

    "13 Hazardous vs non-hazardous asteroid count":
        """
        SELECT is_potentially_hazardous_asteroid, COUNT(*) AS asteroid_count
        FROM asteroids
        GROUP BY is_potentially_hazardous_asteroid
        """,

    "14 Asteroids that passed closer than the Moon (< 1 LD)":
        """
        SELECT a.name, c.close_approach_date, c.miss_distance_lunar
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.miss_distance_lunar < 1
        ORDER BY c.miss_distance_lunar ASC
        """,

    "15 Asteroids that came within 0.05 AU":
        """
        SELECT a.name, c.close_approach_date, c.astronomical
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE c.astronomical < 0.05
        ORDER BY c.astronomical ASC
        """,

    # Additional 5 queries (to reach 20)
    "16 Average diameter of hazardous vs non-hazardous asteroids":
        """
        SELECT is_potentially_hazardous_asteroid,
               ROUND(AVG((estimated_diameter_min_km + estimated_diameter_max_km)/2), 3) AS avg_diameter_km
        FROM asteroids
        GROUP BY is_potentially_hazardous_asteroid
        """,

    "17 Top 10 closest approaches (by km)":
        """
        SELECT a.name, MIN(c.miss_distance_km) AS closest_km
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY a.name
        ORDER BY closest_km ASC
        LIMIT 10
        """,

    "18 Fastest hazardous asteroids":
        """
        SELECT a.name, MAX(c.relative_velocity_kmph) AS max_velocity_kmph
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        WHERE a.is_potentially_hazardous_asteroid = 1
        GROUP BY a.name
        ORDER BY max_velocity_kmph DESC
        LIMIT 10
        """,

    "19 Count of asteroids per year":
        """
        SELECT strftime('%Y', c.close_approach_date) AS year, COUNT(*) AS approach_count
        FROM close_approach c
        GROUP BY year
        ORDER BY year
        """,

    "20 Largest asteroid per year (by max diameter)":
        """
        SELECT strftime('%Y', c.close_approach_date) AS year, a.name,
               MAX(a.estimated_diameter_max_km) AS max_diameter_km
        FROM asteroids a
        JOIN close_approach c ON a.id = c.neo_reference_id
        GROUP BY year
        ORDER BY year
        """
}

# -------------------------
# UI - Sidebar (filters + query selection)
# -------------------------
st.sidebar.header("📋 Controls")

# Database path (allow override)
db_path = st.sidebar.text_input("SQLite DB path", value="nasa_neo.db")

# Filters
st.sidebar.subheader("Filters (applied only in 'Filtered Data' section)")
date_input = st.sidebar.date_input("Close Approach Date (optional)")
vel_min, vel_max = st.sidebar.slider("Velocity km/h range", 0, 200000, (0, 100000))
au_max = st.sidebar.number_input("Max astronomical distance (AU) (leave 0 = ignored)", min_value=0.0, value=0.0, step=0.01, format="%.5f")
lunar_max = st.sidebar.number_input("Max lunar distance (LD) (leave 0 = ignored)", min_value=0.0, value=0.0, step=0.01)
diam_min = st.sidebar.number_input("Min estimated diameter (km)", min_value=0.0, value=0.0, step=0.001)
diam_max = st.sidebar.number_input("Max estimated diameter (km) (0=ignored)", min_value=0.0, value=0.0, step=0.001)
hazardous = st.sidebar.selectbox("Hazardous?", ["All", "Yes", "No"])

st.sidebar.markdown("---")
query_name = st.sidebar.selectbox("Select a predefined query", list(queries.keys()))
run_query = st.sidebar.button("Run Selected Query")

# -------------------------
# Main area
# -------------------------
st.title("🚀 NASA Near-Earth Object (NEO) Tracking & Insights")

conn = get_conn(db_path)

# Run chosen predefined query
if run_query:
    with st.spinner("Running query..."):
        sql = queries[query_name]
        try:
            df = run_sql(sql, conn)
            st.subheader(f"Results — {query_name}")
            st.dataframe(df)
            # Offer CSV download
            csv_bytes = to_csv_bytes(df)
            st.download_button("Download CSV", data=csv_bytes, file_name="query_results.csv", mime="text/csv")
            # Quick chart: choose a numeric column if present
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                col = st.selectbox("Chart: choose numeric column", numeric_cols, key="chart_select")
                st.write(f"Bar chart of `{col}` (grouped by first string column if available)")
                # try grouping by first string column if exists
                str_cols = df.select_dtypes(include="object").columns.tolist()
                if str_cols:
                    group = df.groupby(str_cols[0])[col].mean().sort_values(ascending=False).head(20)
                    st.bar_chart(group)
                else:
                    st.bar_chart(df[col].head(50))
        except Exception as e:
            st.error(f"Query failed: {e}")

st.markdown("---")
st.header("🔎 Filtered Data (apply sidebar filters)")

# Build filtered SQL dynamically from joined tables
base_sql = """
SELECT a.name,
       c.close_approach_date,
       c.relative_velocity_kmph,
       c.astronomical,
       c.miss_distance_km,
       c.miss_distance_lunar,
       a.estimated_diameter_min_km,
       a.estimated_diameter_max_km,
       a.absolute_magnitude_h,
       a.is_potentially_hazardous_asteroid
FROM asteroids a
JOIN close_approach c ON a.id = c.neo_reference_id
WHERE 1=1
"""

# Apply filters
if date_input:
    base_sql += f" AND date(c.close_approach_date) = date('{date_input}')"

base_sql += f" AND c.relative_velocity_kmph BETWEEN {vel_min} AND {vel_max}"

if au_max > 0:
    base_sql += f" AND c.astronomical <= {au_max}"

if lunar_max > 0:
    base_sql += f" AND c.miss_distance_lunar <= {lunar_max}"

if diam_min > 0:
    base_sql += f" AND (a.estimated_diameter_max_km >= {diam_min} OR a.estimated_diameter_min_km >= {diam_min})"

if diam_max > 0:
    base_sql += f" AND (a.estimated_diameter_max_km <= {diam_max} OR a.estimated_diameter_min_km <= {diam_max})"

if hazardous != "All":
    val = 1 if hazardous == "Yes" else 0
    base_sql += f" AND a.is_potentially_hazardous_asteroid = {val}"

base_sql += " ORDER BY c.close_approach_date DESC LIMIT 1000"  # limit to keep interactive

# Fetch and display filtered data
try:
    filtered_df = run_sql(base_sql, conn)
    st.write(f"Showing top {len(filtered_df)} records (limited to 1000).")
    st.dataframe(filtered_df)
    if not filtered_df.empty:
        st.download_button("Download filtered CSV", data=to_csv_bytes(filtered_df),
                           file_name="filtered_neo_data.csv", mime="text/csv")
        # Quick visualizations
        st.subheader("Visualizations")
        # Velocity histogram
        if "relative_velocity_kmph" in filtered_df.columns:
            st.subheader("Relative Velocity distribution")
            st.bar_chart(filtered_df.set_index("name")["relative_velocity_kmph"].head(50))
        # Miss distance scatter if both numeric columns exist
        if {"miss_distance_km", "relative_velocity_kmph"}.issubset(filtered_df.columns):
            st.subheader("Miss distance vs Velocity (sample)")
            scatter_df = filtered_df[["relative_velocity_kmph", "miss_distance_km"]].dropna().head(500)
            st.write(scatter_df)  # small table
except Exception as e:
    st.error(f"Failed to load filtered data: {e}")

st.markdown("---")
st.info("Notes: Put 'nasa_neo.db' in the same folder as this app. For deployment to Streamlit Cloud, upload the DB to the repo (or host it externally).")

# %%
