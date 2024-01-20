import logging
import requests
import json
from urllib.parse import urlparse
from Oort.OortPy import OortPy
from requests.exceptions import Timeout, ConnectionError


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OortLandingZoneManager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.oort = OortPy()

    def ensure_bucket_exists(self):
        """
        Ensure that the specified bucket exists. If it doesn't, create it.
        """
        if self.bucket_name in self.oort.list_buckets():
            print(f"Bucket '{self.bucket_name}' already exists.")
        else:
            self.oort.create_bucket(self.bucket_name)

    def ensure_folder_exists(self, folder):
        """
        Ensure that the specified folder exists in the bucket. 
        If it doesn't, create it.
        This is a basic check; S3-like storages don't have real folder structures, 
        but this checks if any object starts with the given folder path.
        
        :param folder: The folder path to check for.
        :return: True if folder seems to exist, False otherwise.
        """
        objects = self.oort.list_objects(self.bucket_name.lower())
        for obj in objects:
            if obj.startswith(folder):
                return True
    
        # If we reach here, the folder doesn't exist. Let's create it.
        # Adding a trailing slash if not present, to represent it as a folder
        if not folder.endswith('/'):
            folder += '/'
        
        # Creating an empty object with the folder path to simulate folder creation
        self.oort.put_object(self.bucket_name, folder, "")
        logging.info(f"Folder '{folder}' created in '{self.bucket_name}'.")
    
        return True


    # ... [Rest of the class definition]

    def fetch_landing(self, api_url, folder, data_folder, max_retries=3):
        """
        Fetch data from the provided API URL and store it directly into the specified bucket and folder.
        
        :param api_url: URL of the data source.
        :param folder: Main folder inside the bucket.
        :param data_folder: Subfolder inside the main folder where data will be held.
        :param max_retries: Maximum number of retries for fetching data from the API.
        """
        # Extract file name from the API URL
        file_name = urlparse(api_url).path.split('/')[-1]
        
        # Construct the full path with the data folder
        full_path = f"{folder}/{data_folder}/{file_name}"
        
        # Ensure the bucket exists
        self.ensure_bucket_exists()
        
        # Check for existence of the data folder
        if not self.ensure_folder_exists(f"{folder}/{data_folder}/"):
            logging.info(f"Folder '{data_folder}' doesn't exist in '{folder}'. It will be created when the data is stored.")
        
        headers = {
            'User-Agent': 'OortLandingZoneManager/1.0',
            'Accept': 'application/json'
        }
    
        for retry in range(max_retries):
            try:
                response = requests.get(api_url, headers=headers, timeout=10, stream=True)
                response.raise_for_status()
                
                # Attempt to convert response to JSON
                try:
                    data_json = response.json()
                    self.oort.put_object(self.bucket_name, full_path, json.dumps(data_json))
                    logging.info(f"Data successfully stored in {self.bucket_name}/{full_path}")
                    break  # Exit the loop if successful
    
                except json.JSONDecodeError:
                    logging.warning(f"Failed to convert data to JSON for URL: {api_url}. Skipping...")
                    continue
    
            except (Timeout, ConnectionError) as e:
                logging.warning(f"Attempt {retry + 1}/{max_retries} - Error fetching data from API due to {type(e).__name__}. Retrying...")
                time.sleep(2 ** retry)  # Exponential backoff
    
                if retry == max_retries - 1:
                    logging.error(f"Failed to fetch data after {max_retries} attempts.")
                    raise
    
            except requests.HTTPError as e:
                if e.response.status_code == 404:  # Not Found
                    logging.error(f"Resource not found at {api_url}. Skipping retries.")
                    break
                else:
                    logging.error(f"HTTP error when accessing {api_url}: {e}")
                    raise
    
            except requests.RequestException as e:
                logging.error(f"Error fetching data from API: {e}")
                raise
    
            except Exception as e:
                logging.error(f"Error storing data to {self.bucket_name}/{full_path}: {e}")
                raise

    # def fetch_landing(self, api_url, folder, data_folder, max_retries=3):
    #     """
    #     Fetch data from the provided API URL and store it directly into the specified bucket and folder.
        
    #     :param api_url: URL of the data source.
    #     :param folder: Main folder inside the bucket.
    #     :param data_folder: Subfolder inside the main folder where data will be held.
    #     :param max_retries: Maximum number of retries for fetching data from the API.
    #     """
    #     # Extract file name from the API URL
    #     file_name = urlparse(api_url).path.split('/')[-1]
        
    #     # Construct the full path with the data folder
    #     full_path = f"{folder}/{data_folder}/{file_name}"
        
    #     # Ensure the bucket exists
    #     self.ensure_bucket_exists()
        
    #     # Check for existence of the data folder
    #     if not self.ensure_folder_exists(f"{folder}/{data_folder}/"):
    #         logging.info(f"Folder '{data_folder}' doesn't exist in '{folder}'. It will be created when the data is stored.")
        
    #     headers = {
    #         'User-Agent': 'OortLandingZoneManager/1.0',
    #         'Accept': 'application/json'
    #     }
    
    #     for retry in range(max_retries):
    #         try:
    #             # Make the API call with headers and timeout
    #             response = requests.get(api_url, headers=headers, timeout=10, stream=True)
                
    #             # If the response indicates an error, log its content before raising an exception
    #             if response.status_code != 200:
    #                 logging.error(f"Error fetching data from {api_url}. Status code: {response.status_code}. Response: {response.text}")
    #                 response.raise_for_status()
    
    #             # Stream the data directly to the bucket
    #             self.oort.put_object(self.bucket_name, full_path, response.content)
    #             logging.info(f"Data successfully stored in {self.bucket_name}/{full_path}")
    #             break  # Exit the loop if successful
    
    #         except (Timeout, ConnectionError) as e:
    #             logging.warning(f"Attempt {retry + 1}/{max_retries} - Error fetching data from API due to {type(e).__name__}. Retrying...")
    #             # Implementing exponential backoff
    #             time.sleep(2 ** retry)  # This will wait for 2, 4, 8, ... seconds between retries
    #             if retry == max_retries - 1:
    #                 logging.error(f"Failed to fetch data after {max_retries} attempts.")
    #                 raise

    #         except requests.HTTPError as e:
    #             # Handle HTTP errors differently
    #             if e.response.status_code == 404:  # Not Found
    #                 logging.error(f"Resource not found at {api_url}. Skipping retries.")
    #                 break
    #             else:
    #                 logging.error(f"HTTP error when accessing {api_url}: {e}")
    #                 raise

    #         except requests.RequestException as e:
    #             logging.error(f"Error fetching data from API: {e}")
    #             raise
    #         except Exception as e:
    #             logging.error(f"Error storing data to {self.bucket_name}/{full_path}: {e}")
    #             raise
