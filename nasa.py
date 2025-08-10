# In[1]:

import streamlit as st
import pandas as pd

# Page title
st.set_page_config(page_title="NASA NEO Dashboard", layout="wide")
st.title("🚀 NASA Near-Earth Object (NEO) Tracking & Insights")

# Sidebar filters
st.sidebar.header("Filter Data")
date_filter = st.sidebar.date_input("Close Approach Date")
hazardous_filter = st.sidebar.selectbox("Hazardous?", ["All", "True", "False"])
velocity_range = st.sidebar.slider("Velocity (km/h)", 0, 100000, (0, 50000))

# Example dataset (replace with your actual SQL or API data)
data = {
    "name": ["Asteroid A", "Asteroid B", "Asteroid C"],
    "close_approach_date": ["2024-01-01", "2024-02-15", "2024-03-10"],
    "relative_velocity_kmph": [25000, 52000, 43000],
    "is_potentially_hazardous_asteroid": [True, False, True],
}
df = pd.DataFrame(data)

# Filtering
if hazardous_filter != "All":
    df = df[df["is_potentially_hazardous_asteroid"] == (hazardous_filter == "True")]

df = df[
    (df["relative_velocity_kmph"] >= velocity_range[0]) &
    (df["relative_velocity_kmph"] <= velocity_range[1])
]

# Show data
st.subheader("Filtered Asteroids")
st.dataframe(df)

# Simple chart
st.subheader("Velocity Distribution")
st.bar_chart(df.set_index("name")["relative_velocity_kmph"])


# %%
