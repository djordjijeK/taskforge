import time
import unittest

from typing import Optional

from src.taskforge.executor import Executor
from src.taskforge.scheduler import Scheduler
from src.taskforge.task import Task, TaskStatus


class MockTask(Task):
    
    def __init__(
        self,
        execution_time: Optional[float] = None,
        should_fail: Optional[bool] = False,
        tag: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.__execution_time = execution_time
        self.__should_fail = should_fail
        self.__tag = tag


    def execute(self):
        if self.__execution_time:
            time.sleep(self.__execution_time)

        if self.__should_fail:
            raise Exception("Failed!")

        return {"key1": "value1", "key2": "value2"}


    def tag(self) -> str:
        if self.__tag:
            return self.__tag

        return super().tag()
        

class ExecutorTest(unittest.TestCase):


    def setUp(self):
        self.__scheduler = Scheduler()
        self.__executor = Executor(scheduler=self.__scheduler)


    def test_basic_execution_flow(self):
        # task1 -> task2 -> task3
        task1 = MockTask(execution_time=0.1)
        task2 = MockTask(execution_time=0.1, dependencies={task1})
        task3 = MockTask(execution_time=0.1, dependencies={task2})

        for task in [task1, task2, task3]:
            self.__scheduler.schedule(task)

        start_time = time.time()
        self.__executor.run()
        execution_time = time.time() - start_time

        self.assertEqual(TaskStatus.COMPLETED, task1.status)
        self.assertEqual(TaskStatus.COMPLETED, task2.status)
        self.assertEqual(TaskStatus.COMPLETED, task3.status)

        assert task1.result == {"key1": "value1", "key2": "value2"}
        assert task2.result == {"key1": "value1", "key2": "value2"}
        assert task3.result == {"key1": "value1", "key2": "value2"}

        assert execution_time >= 0.3

        for pool in self.__executor._Executor__tag_to_thread_pool.values():
            self.assertTrue(pool._shutdown)


    def test_parallel_execution(self):
        #     task1   task2
        #        \     /
        #         task3
        #           |
        #         task4
        #      /    |    \
        #   task5 task6 task7
        task1 = MockTask(execution_time=0.1)
        task2 = MockTask(execution_time=0.1)
        task3 = MockTask(execution_time=0.1, dependencies={task1, task2})
        task4 = MockTask(execution_time=0.1, dependencies={task3})
        task5 = MockTask(execution_time=0.1, dependencies={task4})
        task6 = MockTask(execution_time=0.1, dependencies={task4})
        task7 = MockTask(execution_time=0.1, dependencies={task4})

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        start_time = time.time()
        self.__executor.run()
        execution_time = time.time() - start_time

        assert execution_time < 0.7
        for task in [task1, task2, task3, task4, task5, task6, task7]:
            assert task.status == TaskStatus.COMPLETED

        for pool in self.__executor._Executor__tag_to_thread_pool.values():
            self.assertTrue(pool._shutdown)


    def test_per_tag_parallel_execution(self):
        task1 = MockTask(execution_time=0.1, tag="TAG1")
        task2 = MockTask(execution_time=0.1, tag="TAG1")
        task3 = MockTask(execution_time=0.1, tag="TAG1")

        task4 = MockTask(execution_time=0.1, tag="TAG2")
        task5 = MockTask(execution_time=0.1, tag="TAG2")
        task6 = MockTask(execution_time=0.1, tag="TAG2")

        for task in [task1, task2, task3, task4, task5, task6]:
            self.__scheduler.schedule(task)

        start_time = time.time()
        self.__executor.run()
        execution_time = time.time() - start_time

        assert execution_time < 0.2
        for task in [task1, task2, task3, task4, task5, task6]:
            assert task.status == TaskStatus.COMPLETED

        for pool in self.__executor._Executor__tag_to_thread_pool.values():
            self.assertTrue(pool._shutdown)


    def test_tasks_cascading_cancellation_if_dependencies_fail(self):
        #     task1   task2
        #        \     /
        #         task3
        #           |
        #         task4
        #      /    |    \
        #   task5 task6 task7
        task1 = MockTask(execution_time=0.1)
        task2 = MockTask(execution_time=0.1, should_fail=True)
        task3 = MockTask(execution_time=0.1, dependencies={task1, task2})
        task4 = MockTask(execution_time=0.1, dependencies={task3})
        task5 = MockTask(execution_time=0.1, dependencies={task4})
        task6 = MockTask(execution_time=0.1, dependencies={task4})
        task7 = MockTask(execution_time=0.1, dependencies={task4})

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        start_time = time.time()
        self.__executor.run()
        execution_time = time.time() - start_time

        assert execution_time < 0.2

        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.FAILED
        for task in [task3, task4, task5, task6]:
            assert task.result is None
            assert task.status == TaskStatus.CANCELED

        for pool in self.__executor._Executor__tag_to_thread_pool.values():
            self.assertTrue(pool._shutdown)