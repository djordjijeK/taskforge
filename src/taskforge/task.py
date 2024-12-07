from __future__ import annotations
from abc import ABC, abstractmethod
from uuid import uuid4
from typing import Optional, Callable
from threading import Event
from enum import Enum


class TaskStatus(Enum):
    PENDING = 'pending'
    SCHEDULED = 'scheduled'
    RUNNING = 'running'
    FAILED = 'failed'
    COMPLETED = 'completed'
    CANCELED = 'canceled'


class Task(ABC):
    """
    Represents a unit of work with dependencies on other tasks.
    The task transitions through states: PENDING -> SCHEDULED -> RUNNING -> [COMPLETED|FAILED|CANCELED].
    """

    def __init__(self, dependencies: Optional[set[Task]] = None, task_id: Optional[str] = None):
        self.__task_id: str = task_id if task_id else str(uuid4())
        self.__task_status: TaskStatus = TaskStatus.PENDING
        self.__dependencies: frozenset[Task] = frozenset(dependencies) if dependencies else frozenset()
        self.__task_result: Optional[object] = None

        self.__canceled_event: Event = Event()

        self.__on_task_completed: Optional[Callable[[Task], None]] = None
        self.__on_task_canceled: Optional[Callable[[Task], None]] = None
        self.__on_task_failed: Optional[Callable[[Task], None]] = None

    @abstractmethod
    def execute(self) -> object:
        """
        Abstract method to run the task's logic.
        Should be overridden by subclass implementations.
        """
        pass

    def tag(self) -> str:
        """
        A tag for categorizing tasks (useful for assigning executors).
        Default is 'default', but can be overridden by subclasses.
        """
        return 'default'

    def execute_task(self):
        """
        Internal method to handle actual task execution.
        Manages status transitions and calls the appropriate callbacks.
        """
        if self.__canceled_event.is_set():
            self.__task_status = TaskStatus.CANCELED
            if self.__on_task_canceled:
                self.__on_task_canceled(self)
            return

        self.__task_status = TaskStatus.RUNNING
        try:
            self.__task_result = self.execute()
            self.__task_status = TaskStatus.COMPLETED
            if self.__on_task_completed:
                self.__on_task_completed(self)
        except Exception as exception:
            self.__task_result = exception
            self.__task_status = TaskStatus.FAILED
            if self.__on_task_failed:
                self.__on_task_failed(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            return False

        return self.__task_id == other.task_id

    def __hash__(self) -> int:
        return hash(self.__task_id)

    def cancel(self) -> None:
        """
        Cancels the task.
        """
        self.__canceled_event.set()

    @property
    def dependencies(self) -> frozenset[Task]:
        return self.__dependencies

    @property
    def status(self) -> TaskStatus:
        return self.__task_status

    @property
    def result(self) -> Optional[object]:
        return self.__task_result

    @property
    def task_id(self) -> str:
        return self.__task_id

    def mark_as_scheduled(self) -> None:
        self.__task_status = TaskStatus.SCHEDULED

    def register_on_task_completed(self, func: Callable[[Task], None]) -> None:
        self.__on_task_completed = func

    def register_on_task_failed(self, func: Callable[[Task], None]) -> None:
        self.__on_task_failed = func

    def register_on_task_canceled(self, func: Callable[[Task], None]) -> None:
        self.__on_task_canceled = func

    def __repr__(self) -> str:
        return  (f"Task(task_id={self.__task_id!r}, "
                f"task_status={self.__task_status.value!r}, "
                f"task_result={self.__task_result}, "
                f"dependencies={[dependency.task_id for dependency in self.__dependencies]!r})")