""" Use the waybackpy library instead """

from waybackpy import WaybackMachineCDXServerAPI



import queue
import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path
import concurrent.futures
import time


class SnapshotOverTime:

    def __init__(self):
        self.timestamps = ['20220901', '20220501', '20220601', '20220801']  # internet archive format is YYYYMMDDhhmmss
        with open('/home/helen_huggingface_co/wayback-machine-scrape/links_scraped_cia_world_factbook.txt', 'r') as f:
            pages = set(f.read().split('\n'))
        self.pages_queue = queue.Queue()
        [self.pages_queue.put(i) for i in pages if i != ' ' and i != ""
         and '#' not in i and "cover-gallery" not in i
         and "images" not in i and 'map' not in i
         and '/flag' not in i
         ]
        print(f"Number of pages in queue: {self.pages_queue.qsize()}")
        self.user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"

    def worker(self):
        while True:
            try:
                page = self.pages_queue.get(timeout=1)
                for t in self.timestamps:
                    txt = ""
                    page_id = page.rstrip('/').split('https://www.cia.gov/the-world-factbook/')[-1].replace("/", "-")
                    print(f"Scraping time {t}, page {page}")

                    # get all snapshots for this page
                    url = page
                    cdx = WaybackMachineCDXServerAPI(url, self.user_agent, start_timestamp=20220101, end_timestamp=t)
                    snapshot_dates = [item.archive_url.split('/')[4] for item in cdx.snapshots()]
                    snapshots = [item.archive_url for item in cdx.snapshots()]

                    # print(f"SNAPSHOT DATES: {snapshot_dates}")
                    # find the closest snapshot prior to the current month
                    closest_prior_snapshot = None
                    for d in snapshot_dates[::-1]:
                        if d[:9] < t:
                            closest_prior_snapshot = d
                            break
                    # if none, just deal
                    if closest_prior_snapshot:
                        snapshot_url = [s for s in snapshots if closest_prior_snapshot in s][0]

                        x = requests.get(snapshot_url)

                        # scrape actual snapshot content
                        html = requests.get(snapshot_url).content
                        soup = BeautifulSoup(html, "html.parser")

                        para = soup.find_all(["p", "h2", "h3"])

                        for br in soup.find_all("br"):
                            br.replace_with("\n")

                        for p in para:
                            if p.name == "h2":
                                txt += (f"\n\nTopic: {p.get_text()}")
                            elif p.name == "h3":
                                txt += (f"\n{p.get_text()}: ")
                            else:
                                txt += (f"{p.get_text()}")

                        page_path = f"factbook/{t}/{page_id}"
                        Path(page_path).mkdir(parents=True, exist_ok=True)
                        with open(f'{page_path}/text.txt', 'w') as f:
                            f.write(txt)
            except Exception as e:
                print(e)
                # print(f"Broken json for page {page_id}: {x.text}")
               return

    def run(self, num_workers=2):
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(self.worker) for _ in range(num_workers)]


s = SnapshotOverTime()
s.run()
