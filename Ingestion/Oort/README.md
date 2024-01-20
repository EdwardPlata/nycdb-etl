# OortPy Package Documentation

`OortPy` is a Python package designed to provide an interface to S3-like storage systems. The primary aim is to streamline operations such as creating, deleting, and listing buckets, as well as performing CRUD operations on objects within those buckets. Accompanying `OortPy` is the `OortLandingZoneManager`, a utility class for managing landing zones within the storage, ensuring proper organization and easy data retrieval.

## Dependencies

- `os`
- `io`
- `boto3`
- `dotenv`
- `botocore`
- `pandas`
- `pandasql`
- `logging`
- `requests`
- `json`
- `urllib.parse`

## OortPy Class

The `OortPy` class provides a wrapper around the boto3 S3 client, enabling easy interaction with S3-compatible storage systems. It uses access keys and an endpoint URL, which are typically stored in environment variables, to initialize the connection.

### Initialization

The class is initialized without any arguments. It automatically reads the necessary credentials from environment variables:

```python
oort_instance = OortPy()

```


---

## Core Methods of `OortPy`

### Core Methods

1. **create_bucket(bucket_name: str)**: Creates a new bucket.
2. **delete_bucket(bucket_name: str)**: Deletes a specified bucket.
3. **list_buckets()**: Lists all buckets.
4. **list_objects(bucket_name: str)**: Lists all objects in a specified bucket.
5. **put_object(bucket_name: str, key: str, data: str)**: Puts an object into a specified bucket.
6. **get_object(bucket_name: str, key: str)**: Retrieves an object from a specified bucket.
7. **delete_object(bucket_name: str, key: str)**: Deletes an object from a specified bucket.
8. **query_csv(bucket_name: str, csv_key: str)**: Queries a CSV file in a specified bucket and returns the result as a Pandas DataFrame.
9. **query_csv_pandasql(bucket_name: str, csv_key: str, query: str)**: Uses SQL to query a CSV file in a specified bucket and returns the result as a Pandas DataFrame.
10. **list_objects_v2(bucket_name: str)**: Lists all objects in a specified bucket using pagination.

## OortLandingZoneManager Class

`OortLandingZoneManager` is a utility class that manages landing zones within the storage. It ensures that the specified bucket exists and provides functionalities like ensuring folders exist within buckets and fetching data to store directly in the landing zone.

### Initialization

The manager is initialized with a bucket name:

```python
manager = OortLandingZoneManager(bucket_name="your_bucket_name")

```


---

## Core Methods of `OortLandingZoneManager`
### Core Methods

1. **ensure_bucket_exists()**: Ensures the specified bucket exists. If not, it creates one.
2. **ensure_folder_exists(folder: str)**: Ensures a specific folder exists within the bucket.
3. **fetch_landing(api_url: str, folder: str, data_folder: str, max_retries: int)**: Fetches data from an API URL and stores it directly into the specified bucket and folder.

## Usage

After initializing the `OortPy` or `OortLandingZoneManager` class, you can call any of the methods as required. For instance, to create a bucket and list all existing buckets:

```python
oort_instance = OortPy()
oort_instance.create_bucket("new_bucket_name")
all_buckets = oort_instance.list_buckets()
print(all_buckets)

manager = OortLandingZoneManager(bucket_name="your_bucket_name")
manager.ensure_bucket_exists()
manager.fetch_landing(api_url="http://data_source_url.com/data.csv", folder="landing_zone", data_folder="data_set_1")

```