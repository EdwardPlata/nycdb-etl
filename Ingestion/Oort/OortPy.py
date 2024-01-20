import os
import io
import boto3
from dotenv import load_dotenv
import botocore
import pandas as pd
from pandasql import sqldf

class OortPy:
    def __init__(self):
        """
        Initialize the OortWrapper with access keys and endpoint URL.
        """
        load_dotenv()
        access_key = os.getenv('OORT_ACCESS_KEY')
        secret_key = os.getenv('OORT_SECRET_KEY')
        endpoint_url = os.getenv('OORT_ENDPOINT_URL')

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url
        )

    def create_bucket(self, bucket_name):
        """
        Create a new bucket with the given name.

        :param bucket_name: The name of the bucket to create.
        :return: The response from the create_bucket API call.
        """
        try:
            return self.s3_client.create_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists' or e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print(f"Bucket '{bucket_name}' already exists. Skipping bucket creation.")
            else:
                raise e

    def delete_bucket(self, bucket_name):
        """
        Delete the specified bucket.

        :param bucket_name: The name of the bucket to delete.
        :return: The response from the delete_bucket API call.
        """
        try:
            return self.s3_client.delete_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            print(f"Error deleting bucket '{bucket_name}': {e}")
            raise e

    def list_buckets(self):
        """
        List all buckets.

        :return: A list of bucket names.
        """
        response = self.s3_client.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]

    def list_objects(self, bucket_name):
        """
        List all objects in the specified bucket.

        :param bucket_name: The name of the bucket to list objects from.
        :return: A list of object keys.
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [item['Key'] for item in response['Contents']]
        else:
            return []

    def put_object(self, bucket_name, key, data):
        """
        Put an object into the specified bucket.

        :param bucket_name: The name of the bucket to store the object in.
        :param key: The key (name) of the object.
        :param data: The data (content) of the object.
        :return: The response from the put_object API call.
        """
        try:
            return self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
        except botocore.exceptions.ClientError as e:
            print(f"Error putting object '{key}' in bucket '{bucket_name}': {e}")
            raise e

    def get_object(self, bucket_name, key):
        """
        Get an object from the specified bucket.

        :param bucket_name: The name of the bucket to retrieve the object from.
        :param key: The key (name) of the object.
        :return: The data (content) of the object.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return response['Body'].read()
        except botocore.exceptions.ClientError as e:
            print(f"Error getting object '{key}' from bucket '{bucket_name}': {e}")
            raise e

    def delete_object(self, bucket_name, key):
        """
        Delete an object from the specified bucket.

        :param bucket_name: The name of the bucket to delete the object from.
        :param key: The key (name) of the object.
        :return: The response from the delete_object API call.
        """
        try:
            return self.s3_client.delete_object(Bucket=bucket_name, Key=key)
        except botocore.exceptions.ClientError as e:
            print(f"Error deleting object '{key}' from bucket '{bucket_name}': {e}")
            raise e


    def query_csv(self, bucket_name, csv_key):
        """
        Query a CSV file in the specified bucket and return the result as a Pandas DataFrame.

        :param bucket_name: The name of the bucket containing the CSV file.
        :param csv_key: The key (name) of the CSV file.
        :return: A Pandas DataFrame containing the result of the query.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=csv_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
            return df
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"Bucket '{bucket_name}' does not exist.")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print(f"CSV file with key '{csv_key}' does not exist in bucket '{bucket_name}'.")
            else:
                print(f"Error querying CSV file '{csv_key}' from bucket '{bucket_name}': {e}")
            raise e
        
    def query_csv_pandasql(self, bucket_name, csv_key, query):
        """
        Query a CSV file in the specified bucket using SQL and return the result as a Pandas DataFrame.

        :param bucket_name: The name of the bucket containing the CSV file.
        :param csv_key: The key (name) of the CSV file.
        :param query: The SQL query to execute on the CSV file.
        :return: A Pandas DataFrame containing the result of the query.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=csv_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
            result = sqldf(query, locals())
            return result
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"Bucket '{bucket_name}' does not exist.")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print(f"CSV file with key '{csv_key}' does not exist in bucket '{bucket_name}'.")
            else:
                print(f"Error querying CSV file '{csv_key}' from bucket '{bucket_name}': {e}")
            raise e

    def list_objects_v2(self, bucket_name):
        """
        List all objects in the specified bucket using pagination.
    
        :param bucket_name: The name of the bucket to list objects from.
        :return: A list of object keys.
        """
        # Start with an empty continuation token
        continuation_token = None
        
        # List to store all object keys
        all_object_keys = []
        
        while True:
            # Include the continuation token in the call if it's available
            if continuation_token:
                response = self.s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
            else:
                response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            
            # If 'Contents' is in the response, extract the object keys
            if 'Contents' in response:
                all_object_keys.extend([item['Key'] for item in response['Contents']])
            
            # If there's no 'NextContinuationToken' in the response, break out of the loop
            if 'NextContinuationToken' not in response:
                break
            
            # Otherwise, update the continuation token for the next loop iteration
            continuation_token = response['NextContinuationToken']
        
        return all_object_keys
