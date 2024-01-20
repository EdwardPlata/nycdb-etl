import asyncio
from nyc_data_pipeline.source_extract_async import NYCPublicDataFetcher, NYCEndpointFetcher
from Oort.OortLandingManager import OortLandingZoneManager
from orchestration.pipeline_utils import *

class NYCPipeline:

    def __init__(self, urls, bucket_name):
        self.urls = urls
        self.bucket_name = bucket_name
        self.endpoints = {}
        self.manager = OortLandingZoneManager(bucket_name=self.bucket_name)
        self.pipeline_manager = UnifiedPipelineManager()

    async def fetch_endpoints_from_url(self, url):
        data_fetcher = NYCPublicDataFetcher(url)
        data_dict = await data_fetcher.run()
        data_dict = {url.split('/')[-2]: url.lower() for _, url in data_dict.items()}
        endpoint_fetcher = NYCEndpointFetcher()
        endpoints = await endpoint_fetcher.run(data_dict)
        return endpoints

    async def fetch_all_endpoints(self):
        tasks = [self.fetch_endpoints_from_url(url) for url in self.urls]
        results = await asyncio.gather(*tasks)
        for endpoints in results:
            self.endpoints.update(endpoints)
    
    def store_data(self, api_url, key):
        try:
            self.manager.fetch_landing(api_url, folder="landingzone", data_folder=key)
        except Exception as e:
            print(f"Error while storing data for {key}: {e}")

    def run(self):
        asyncio.run(self.fetch_all_endpoints())
        for key, api_url in self.endpoints.items():
            event = Event({"api_url": api_url, "key": key})
            self.pipeline_manager.enqueue_event(event)
        while not self.pipeline_manager.is_empty():
            event_data = self.pipeline_manager.dequeue_event().data
            self.store_data(event_data["api_url"], event_data["key"])

