import streamlit as st
from streamlit_ace import st_ace
import time
import os


# Configure Streamlit Page
page_icon = "assets/img/database.svg"
st.set_page_config(
    page_title="Blockchain Query Editor", page_icon=page_icon, layout="wide"
)

# Read Custom CSS
with open("assets/css/style.css", "r") as f:
    css_text = f.read()
custom_css = f"<style>{css_text}</style>"
st.markdown(custom_css, unsafe_allow_html=True)


# Get API Keys
flipside_api_key = os.environ["FLIPSIDE_KEY"]
transpose_api_key = os.environ["TRANSPOSE_KEY"]
chainbase_api_key = os.environ["CHAINBASE_KEY"]

# Fetch data
schema_df = load_data()

# Render the Query Editor
table_values = sorted(schema_df["Tables"].unique())

# Sidebar
st.sidebar.image("assets/img/cusp.png", width=150)
st.sidebar.header("Query CUSP Data")
for table_name in table_values:
    with st.sidebar.expander(table_name):
        st.code(f"`censusdata-team3si.elds_db.{table_name}`", language="sql")
        columns_df = schema_df[schema_df["Tables"] == table_name][["Columns"]]
        st.table(columns_df)

# Query Editor
ace_query = st_ace(
    language="sql",
    placeholder="SELECT * FROM `censusdata-team3si.elds_db.acs` limit 10",
    theme="twilight",
)

if ace_query:
    # Create an empty element to hold the progress bar
    progress_bar = st.empty()

    # Execute the query and update the progress bar
    progress_text = "Executing Query..."
    for percent_complete in range(100):
        time.sleep(0.05)
        progress_bar.progress(percent_complete + 1, text=progress_text)

    # Hide the progress bar and display a message
    progress_bar.empty()

    # Connect to the database and execute the query
    results_df = query_gcp(ace_query)

    # Display the results
    st.write(results_df)
