"""
For each domain in `links_scraped_cia_world_factbook.txt`, for each of <May 2022, June/July 2022, August 2022>,
get the first day of the month and find the closest snapshot according to the Wayback Machine, then call Wayback Machine
API again with the timestamp to get the actual content dump. Content dump is saved to list of dicts, which is then
converted to Pandas and then Parquet on disk.
"""

import requests
from bs4 import BeautifulSoup
import json

# api = 'http://archive.org/wayback/available?url=example.com'

with open('/home/helen_huggingface_co/wayback-machine-scrape/links_scraped_cia_world_factbook.txt', 'r') as f:
    pages = f.read().split('\n')

timestamps = ['20220501', '20220601', '20220801']  # internet archive format is YYYYMMDDhhmmss

txt = ""
# TODO: multithread this; each worker picks up a (timestamp, page) tuple and pings the API
for i, page in enumerate(pages):
    for t in timestamps:
        x = requests.get(f'http://archive.org/wayback/available?url={page}&timestamp={t}')
        metadata_dict = json.loads(x.text)
        closest_timestamp = metadata_dict['timestamp']
        snapshot_url = metadata_dict['url']

        # scrape actual snapshot content
        html = requests.get(snapshot_url).content
        soup = BeautifulSoup(html, "html.parser")

        para = soup.find_all(["p", "h2", "h3"])
        # txt += "".join([str(i) for i in para]) + '\n\n\n'  # figure out how to format text bc headers all over the place

        for br in soup.find_all("br"):
            br.replace_with("\n")

        for p in para:
            if p.name == 'h2':
                txt += (f"\n\nTopic: {p.get_text()}")
            else:
                txt += (f"{p.get_text()}")

        """
        todo: for each timestamped snapshot, get the text which is different than the previous bit and where it should
        correspond to, if any 
        """