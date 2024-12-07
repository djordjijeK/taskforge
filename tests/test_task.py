import unittest

from unittest.mock import Mock

from src.taskforge.task import Task, TaskStatus


class MockTask(Task):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def execute(self):
        return {"key1": "value1", "key2": "value2"}


class TaskTest(unittest.TestCase):


    def setUp(self):
        self.__default_test_task = MockTask()

        self.__mocked_completed_callback = Mock()
        self.__mocked_failed_callback = Mock()
        self.__mocked_canceled_callback = Mock()

        self.__default_test_task.register_on_task_completed(self.__mocked_completed_callback)
        self.__default_test_task.register_on_task_failed(self.__mocked_failed_callback)
        self.__default_test_task.register_on_task_canceled(self.__mocked_canceled_callback)


    def test_task_initial_state(self):
        assert self.__default_test_task.status == TaskStatus.PENDING
        assert self.__default_test_task.dependencies == set()
        assert self.__default_test_task.tag() == "default"

        self.__mocked_completed_callback.assert_not_called()
        self.__mocked_failed_callback.assert_not_called()
        self.__mocked_canceled_callback.assert_not_called()


    def test_task_successful_execution(self):
        self.__default_test_task.execute_task()

        assert self.__default_test_task.status == TaskStatus.COMPLETED
        assert self.__default_test_task.result == {"key1": "value1", "key2": "value2"}

        self.__mocked_completed_callback.assert_called_once_with(self.__default_test_task)
        self.__mocked_failed_callback.assert_not_called()
        self.__mocked_canceled_callback.assert_not_called()


    def test_task_failed_execution(self):
        def execute():
            raise ValueError("Exception!!!")

        self.__default_test_task.execute = execute

        self.__default_test_task.execute_task()

        assert self.__default_test_task.status == TaskStatus.FAILED
        assert isinstance(self.__default_test_task.result, ValueError)
        assert str(self.__default_test_task.result) == "Exception!!!"

        self.__mocked_completed_callback.assert_not_called()
        self.__mocked_failed_callback.assert_called_once_with(self.__default_test_task)
        self.__mocked_canceled_callback.assert_not_called()


    def test_task_cancellation(self):
        self.__default_test_task.cancel()
        self.__default_test_task.execute_task()

        assert self.__default_test_task.status == TaskStatus.CANCELED
        assert self.__default_test_task.result is None

        self.__mocked_completed_callback.assert_not_called()
        self.__mocked_failed_callback.assert_not_called()
        self.__mocked_canceled_callback.assert_called_once_with(self.__default_test_task)


    def test_task_dependencies_and_tags(self):
        def tag():
            return "tag"

        task1 = MockTask()
        task2 = MockTask()
        task3 = MockTask(dependencies={task1, task2, task1, task2})

        task1.tag = tag
        task2.tag = tag

        assert task1.dependencies == set()
        assert task2.dependencies == set()
        assert task3.dependencies == {task1, task2}

        assert task1.tag() == "tag"
        assert task2.tag() == "tag"
        assert task3.tag() == "default"


    def test_task_equality(self):
        task1 = MockTask(task_id="same_id")
        task2 = MockTask(task_id="same_id")
        task3 = MockTask(task_id="different_id")

        assert task1 == task2
        assert task1 != task3
        assert len({task1, task2, task3}) == 2


    def test_task_dependencies_are_immutable(self):
        dependency = MockTask()
        task = MockTask(dependencies={dependency})

        with self.assertRaises(Exception):
            task.dependencies.add(MockTask())


    def test_task_id_generation(self):
        task1 = MockTask()
        task2 = MockTask()

        assert task1 != task2