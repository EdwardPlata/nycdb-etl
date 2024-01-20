# NYC Data Pipeline

The `NYCPipeline` is a data orchestration class designed to fetch and store data from NYC public datasets into the Oort Landing Zone. It utilizes asynchronous data fetching to efficiently gather data endpoints from a list of URLs and then stores the corresponding data into the specified bucket.

## Dependencies

- `asyncio`
- `nyc_data_pipeline.source_extract_async`: Contains `NYCPublicDataFetcher` and `NYCEndpointFetcher` for fetching public data from NYC.
- `Oort.OortLandingManager`: Handles operations with the Oort Landing Zone.
- `orchestration.pipeline_utils`: Provides utilities for the pipeline orchestration.

## Initialization

The pipeline is initialized with a list of URLs pointing to NYC public datasets and a bucket name for the Oort Landing Zone:

```python
pipeline = NYCPipeline(urls=["url1", "url2"], bucket_name="your_bucket_name")

---
```

## Method

### `fetch_endpoints_from_url(url: str) -> dict`

An asynchronous method that fetches data endpoints for a given URL. It returns a dictionary of endpoints.

### `fetch_all_endpoints()`

An asynchronous method that fetches all data endpoints for the list of URLs provided during initialization. The results are stored in the `endpoints` attribute of the class.

### `store_data(api_url: str, key: str)`

This method tries to store the data corresponding to a given API URL into the Oort Landing Zone. The `key` parameter represents the data's unique identifier or name.

### `run()`

This is the main method that orchestrates the entire pipeline. It performs the following steps:

1. Fetches all data endpoints asynchronously.
2. Enqueues each data endpoint into the pipeline manager.
3. Dequeues each event from the pipeline manager and stores the corresponding data into the Oort Landing Zone.

## Usage

```python
# Initialize the pipeline
pipeline = NYCPipeline(urls=["url1", "url2"], bucket_name="your_bucket_name")

# Run the pipeline
pipeline.run()
```
Please ensure you replace placeholders like "url1", "url2" and "your_bucket_name" with actual values when utilizing the provided examples.