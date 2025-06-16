import threading
from urllib.parse import urljoin

from config import logger, settings
from core.request import Request


class LSEG:

    THREAD_COUNT = settings.THREAD_COUNT
    BASE_URL = "https://www.lseg.com/bin/esg/"

    def __init__(self):
        logger.info("Initializing LSEG instance")
        self.request = Request().request
        self.result = {}

    def run(self):
        logger.info("LSEG run started")
        self.fetch_tickers()
        self.tasks = self.rics.copy()
        logger.info("Fetched %d RICs", len(self.rics))
        self.start_workers()
        logger.info("All threads have completed")
        return self.result

    def fetch_tickers(self):
        url = urljoin(self.BASE_URL, "esgsearchsuggestions/")
        logger.info("Fetching tickers from %s", url)
        try:
            resp = self.request("GET", url)
            resp.raise_for_status()
            self.rics = resp.json()
            logger.info("Successfully fetched and trimmed tickers to top 100")
        except Exception as e:
            logger.error("Failed to fetch tickers: %s", str(e))
            raise

    def start_workers(self):
        logger.info("Starting %d worker threads", self.THREAD_COUNT)
        threads = []
        for i in range(self.THREAD_COUNT):
            t = threading.Thread(target=self.worker, name=f"Worker-{i}")
            threads.append(t)
            t.start()
            logger.debug("Started thread %s", t.name)

        for t in threads:
            t.join()
            logger.debug("Thread %s has finished", t.name)

    def worker(self):
        logger.debug("Thread %s: started", threading.current_thread().name)
        while self.tasks:
            try:
                ric_data = self.tasks.pop(0)
            except IndexError:
                logger.debug("Thread %s: no more tasks", threading.current_thread().name)
                return

            ric = tuple(ric_data.values())
            if ric in self.result:
                logger.debug("Thread %s: skipping duplicate RIC %s", threading.current_thread().name, ric)
                continue

            try:
                logger.debug("Thread %s: fetching ESG scores for RIC %s", threading.current_thread().name, ric[1])
                self.result[ric] = self.fetch_esg_scores(ric[1])
                logger.info("Thread %s: fetched data for %s", threading.current_thread().name, ric[1])
            except Exception as e:
                logger.warning("Thread %s: Unable to fetch data for %s: %s",
                               threading.current_thread().name, ric[1], str(e))

    def fetch_esg_scores(self, ric):
        url = urljoin(self.BASE_URL, "esgsearchresult/")
        params = {"ricCode": ric}
        logger.debug("Fetching ESG scores from %s with params %s", url, params)
        resp = self.request("GET", url, params=params)
        resp.raise_for_status()
        return resp.json()
