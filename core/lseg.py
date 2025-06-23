import multiprocessing
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
        self.tasks = []

    def run(self):
        logger.info("LSEG run started")
        self.fetch_tickers()
        self.tasks = self.rics.copy()
        logger.info("Fetched %d RICs", len(self.rics))
        self.start_workers()
        logger.info("All processes and threads have completed")
        return dict(self.result)

    def fetch_tickers(self):
        url = urljoin(self.BASE_URL, "esgsearchsuggestions/")
        logger.info("Fetching tickers from %s", url)
        try:
            resp = self.request("GET", url)
            resp.raise_for_status()
            self.rics = resp.json()
            logger.info("Successfully fetched tickers")
        except Exception as e:
            logger.error("Failed to fetch tickers: %s", str(e))
            raise

    def start_workers(self):
        manager = multiprocessing.Manager()
        self.tasks = manager.list(self.tasks)
        self.result = manager.dict()
        lock = manager.RLock()
        n_proc = multiprocessing.cpu_count()
        logger.info(
            "Starting %d processes × %d threads each", n_proc, self.THREAD_COUNT
        )
        processes = [
            multiprocessing.Process(
                target=self._process_target,
                name=f"Proc-{i}",
                args=(lock,),
            )
            for i in range(n_proc)
        ]

        for p in processes:
            p.start()
            logger.debug("Started %s", p.name)

        for p in processes:
            p.join()
            logger.debug("%s has finished", p.name)

    def _process_target(self, lock):
        local_threads = [
            threading.Thread(
                target=self.worker,
                name=f"{multiprocessing.current_process().name}-T{t}",
                args=(lock,),
                daemon=True,
            )
            for t in range(self.THREAD_COUNT)
        ]

        for t in local_threads:
            t.start()

        for t in local_threads:
            t.join()

    def worker(self, lock):
        thread_name = threading.current_thread().name
        logger.debug("%s: started", thread_name)

        while True:
            with lock:
                if not self.tasks:
                    logger.debug("%s: no more tasks", thread_name)
                    break
                ric_data = self.tasks.pop(0)

            ric = tuple(ric_data.values())
            if ric in self.result:
                logger.debug("%s: skipping duplicate RIC %s", thread_name, ric)
                continue

            try:
                logger.debug("%s: fetching ESG scores for %s", thread_name, ric[1])
                data = self.fetch_esg_scores(ric[1])
                with lock:
                    self.result[ric] = data
                logger.debug("%s: fetched data for %s", thread_name, ric[1])
            except Exception as e:
                logger.warning(
                    "%s: unable to fetch data for %s: %s", thread_name, ric[1], e
                )

    def fetch_esg_scores(self, ric):
        url = urljoin(self.BASE_URL, "esgsearchresult/")
        params = {"ricCode": ric}
        logger.debug("Fetching ESG scores from %s with params %s", url, params)
        resp = self.request("GET", url, params=params)
        resp.raise_for_status()
        return resp.json()
