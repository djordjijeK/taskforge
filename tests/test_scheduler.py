import random
import unittest

from collections import defaultdict

from src.taskforge.task import Task, TaskStatus
from src.taskforge.scheduler import Scheduler, SchedulingException


class MockTask(Task):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def execute(self):
        return {"key1": "value1", "key2": "value2"}


class TestScheduler(unittest.TestCase):


    def setUp(self):
        self.__scheduler = Scheduler()


    def test_scheduler_initial_state(self):
        self.assertEqual(defaultdict(set), self.__scheduler._Scheduler__task_to_dependencies)
        self.assertEqual(defaultdict(set), self.__scheduler._Scheduler__task_to_dependents)


    def test_duplicate_task_scheduling(self):
        task = MockTask()
        self.__scheduler.schedule(task)

        with self.assertRaises(SchedulingException) as context:
            self.__scheduler.schedule(task)

        self.assertIn("already exists", str(context.exception))


    def test_task_with_dependencies(self):
        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})

        self.__scheduler.schedule(task1)
        self.__scheduler.schedule(task2)
        self.__scheduler.schedule(task3)

        self.assertEqual({task1, task2}, self.__scheduler._Scheduler__task_to_dependencies[task3])

        self.assertIn(task3, self.__scheduler._Scheduler__task_to_dependents[task1])
        self.assertIn(task3, self.__scheduler._Scheduler__task_to_dependents[task2])


    def test_cycle_detection_simple(self):
        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})
        task4 = MockTask(dependencies={task3})
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task4})
        task7 = MockTask(dependencies={task4})

        task1._Task__dependencies = {task7}

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        with self.assertRaises(SchedulingException) as context:
            next(self.__scheduler.ready_tasks)
        self.assertIn("circular dependencies", str(context.exception))


    def test_task_execution_order_1(self):
        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})
        task4 = MockTask(dependencies={task3})
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task4})
        task7 = MockTask(dependencies={task4})

        tasks = [task1, task2, task3, task4, task5, task6, task7]
        random.shuffle(tasks)
        for task in tasks:
            self.__scheduler.schedule(task)

        ready_tasks = self.__scheduler.ready_tasks
        expected_sequences = [
            {task1, task2}, {task1, task2}, {task3}, {task4},
            {task5, task6, task7}, {task5, task6, task7}, {task5, task6, task7}
        ]

        for expected in expected_sequences:
            task = next(ready_tasks)
            assert task in expected
            task.execute_task()


    def test_task_execution_order_2(self):
        #       A
        #    /  |  \
        #   B1  B2  B3
        #    \  |  /
        #      C1
        #      | \
        #      C2 D
        #       \ /
        #        E
        taskA = MockTask(task_id="taskA")
        taskB1 = MockTask(task_id="taskB1", dependencies={taskA})
        taskB2 = MockTask(task_id="taskB2", dependencies={taskA})
        taskB3 = MockTask(task_id="taskB3", dependencies={taskA})
        taskC = MockTask(task_id="taskC", dependencies={taskB1, taskB2, taskB3})
        taskD1 = MockTask(task_id="taskD1", dependencies={taskC})
        taskD2 = MockTask(task_id="taskD2", dependencies={taskC})
        taskE = MockTask(task_id="taskE", dependencies={taskD1, taskD2})

        tasks = [taskA, taskB1, taskB2, taskB3, taskC, taskD1, taskD2, taskE]
        random.shuffle(tasks)
        for task in tasks:
            self.__scheduler.schedule(task)

        ready_tasks = self.__scheduler.ready_tasks
        expected_sequences = [
            {taskA}, {taskB1, taskB2, taskB3}, {taskB1, taskB2, taskB3}, {taskB1, taskB2, taskB3},
            {taskC}, {taskD1, taskD2}, {taskD1, taskD2}, {taskE}
        ]

        for expected in expected_sequences:
            task = next(ready_tasks)
            assert task in expected
            task.execute_task()


    def test_multiple_independent_tasks(self):
        tasks = [MockTask() for i in range(5)]
        for task in tasks:
            self.__scheduler.schedule(task)

        ready_tasks = set()
        while True:
            try:
                ready_tasks.add(next(self.__scheduler.ready_tasks))
            except StopIteration:
                break

        self.assertEqual(set(tasks), ready_tasks)
        self.assertTrue(all(task.status == TaskStatus.SCHEDULED for task in tasks))


    def test_parallel_dependency_chains(self):
        # Chain 1: task1 -> task2 -> task3
        task1 = MockTask()
        task2 = MockTask(dependencies={task1})
        task3 = MockTask(dependencies={task2})

        # Chain 2: task4 -> task5 -> task6
        task4 = MockTask()
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task5})

        tasks = [task1, task2, task3, task4, task5, task6]
        for task in tasks:
            self.__scheduler.schedule(task)

        ready_tasks = set()
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        self.assertEqual({task1, task4}, ready_tasks)

        task1.execute_task()
        task4.execute_task()

        ready_tasks = set()
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        self.assertEqual({task2, task5}, ready_tasks)


    def test_diamond_dependency_pattern(self):
        #      task1
        #     /     \
        # task2     task3
        #     \     /
        #      task4
        task1 = MockTask()
        task2 = MockTask(dependencies={task1})
        task3 = MockTask(dependencies={task1})
        task4 = MockTask(dependencies={task2, task3})

        tasks = [task1, task2, task3, task4]
        for task in tasks:
            self.__scheduler.schedule(task)

        # First ready task should be task1
        ready_task = next(self.__scheduler.ready_tasks)
        self.assertEqual(task1, ready_task)

        task1.execute_task()

        # Next ready tasks should be task2 and task3 (in any order)
        ready_tasks = set()
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        ready_tasks.add(next(self.__scheduler.ready_tasks))
        self.assertEqual({task2, task3}, ready_tasks)

        task2.execute_task()
        task3.execute_task()

        # Final ready task should be task4
        ready_task = next(self.__scheduler.ready_tasks)
        self.assertEqual(task4, ready_task)


    def test_failure_propagation_1(self):
        def execute():
            raise Exception("Failed")

        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})
        task4 = MockTask(dependencies={task3})
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task4})
        task7 = MockTask(dependencies={task4})

        task1.execute = execute

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        for task in self.__scheduler.ready_tasks:
            task.execute_task()

        assert task1.status == TaskStatus.FAILED
        assert task2.status == TaskStatus.COMPLETED

        for task in [task3, task4, task5, task6, task7]:
            assert task.status == TaskStatus.CANCELED


    def test_failure_propagation_2(self):
        def execute():
            raise Exception("Failed")

        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})
        task4 = MockTask(dependencies={task3})
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task4})
        task7 = MockTask(dependencies={task4})

        task3.execute = execute

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        for task in self.__scheduler.ready_tasks:
            task.execute_task()

        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.COMPLETED
        assert task3.status == TaskStatus.FAILED

        for task in [task4, task5, task6, task7]:
            assert task.status == TaskStatus.CANCELED


    def test_failure_propagation_3(self):
        def execute():
            raise Exception("Failed")

        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2})
        task4 = MockTask(dependencies={task3})
        task5 = MockTask(dependencies={task4})
        task6 = MockTask(dependencies={task4})
        task7 = MockTask(dependencies={task4})

        task4.execute = execute

        for task in [task1, task2, task3, task4, task5, task6, task7]:
            self.__scheduler.schedule(task)

        for task in self.__scheduler.ready_tasks:
            task.execute_task()

        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.COMPLETED
        assert task3.status == TaskStatus.COMPLETED
        assert task4.status == TaskStatus.FAILED

        for task in [task5, task6, task7]:
            assert task.status == TaskStatus.CANCELED