from concurrent.futures.thread import ThreadPoolExecutor
from .scheduler import Scheduler
from typing import Dict


class Executor:

    def __init__(self, scheduler: Scheduler, workers_per_tag: int = 3):
        self.__scheduler: Scheduler = scheduler
        self.__workers_per_tag = workers_per_tag

        self.__tag_to_thread_pool: Dict[str, ThreadPoolExecutor] = {}


    def __ensure_thread_pool(self, tag: str) -> ThreadPoolExecutor:
        if tag not in self.__tag_to_thread_pool:
            self.__tag_to_thread_pool[tag] = ThreadPoolExecutor(max_workers=self.__workers_per_tag, thread_name_prefix=tag)

        return self.__tag_to_thread_pool[tag]


    def run(self):
        try:
            for task in self.__scheduler.ready_tasks:
                thread_pool = self.__ensure_thread_pool(task.tag())
                thread_pool.submit(task.execute_task)
        finally:
            self.shutdown()


    def shutdown(self, wait: bool = True):
        for thread_pool in self.__tag_to_thread_pool.values():
            thread_pool.shutdown(wait=wait)
