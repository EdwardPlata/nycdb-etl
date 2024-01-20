# analytics_app.py

import streamlit as st
from nyc_data_pipeline import NYCPipeline  # Make sure to import your pipeline
from Oort.OortPy import OortPy  # Import the OortPy class

oort = OortPy()

def run_pipeline(categorical_urls):
    pipeline = NYCPipeline(categorical_urls, selected_bucket)
    pipeline.run()
    st.success("Pipeline run completed!")

st.title("NYC Data Analytics Dashboard")

# Sidebar for Oort operations
st.sidebar.header("Oort Operations")

# List out all buckets and ability to select one
buckets = oort.list_buckets()
selected_bucket = st.sidebar.selectbox("Select a bucket", buckets)

# Allow user to create a new bucket
new_bucket_name = st.sidebar.text_input("Create a new bucket:", "")
if new_bucket_name and st.sidebar.button("Create"):
    oort.create_bucket(new_bucket_name)
    st.sidebar.success(f"Bucket {new_bucket_name} created!")

# Allow user to provide categorical URLs or any other queries
st.sidebar.header("Data Pipeline")
categorical_urls_input = st.sidebar.text_area("Enter Categorical URLs (comma-separated)", "")
categorical_urls = [url.strip() for url in categorical_urls_input.split(",")]
if st.sidebar.button("Run Pipeline"):
    run_pipeline(categorical_urls)

# Your analytics visualizations and components go here
st.header("Analytics Visualization")
st.write("Your visualizations will appear here.")
