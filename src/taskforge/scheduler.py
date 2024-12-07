from threading import Condition
from collections import defaultdict, deque
from .task import Task, TaskStatus
from typing import Optional, Generator


class SchedulingException(Exception):
    pass


class Scheduler:
    def __init__(self, tasks: Optional[set[Task]] = None) -> None:
        # Track dependencies in both directions for efficient graph traversal
        self.__task_to_dependencies: dict[Task, set[Task]] = defaultdict(set)  # Task -> set of its dependencies
        self.__task_to_dependents: dict[Task, set[Task]] = defaultdict(set)  # Task -> set of tasks that depend on it
        self.__condition = Condition()  # Threading condition for coordination between task state changes

        for task in tasks if tasks else set():
            self.schedule(task)

    def schedule(self, task: Task) -> None:
        if task in self.__task_to_dependencies:
            raise SchedulingException(f"Task {task} already exists in the scheduler.")

        # Register callbacks for task state changes
        task.register_on_task_completed(self.__on_task_completed)
        task.register_on_task_failed(self.__on_task_failed)
        task.register_on_task_canceled(self.__on_task_canceled)

        # Build bidirectional dependency graph
        self.__task_to_dependencies[task] = set(task.dependencies)
        for dependency in task.dependencies:
            self.__task_to_dependents[dependency].add(task)

    def __on_task_completed(self, task: Task) -> None:
        with self.__condition:
            self.__condition.notify_all()  # Wake up ready_tasks iterator to check for newly available tasks

    def __on_task_failed(self, task: Task) -> None:
        # Cancel all dependent tasks when a dependency fails
        for dependent in self.__task_to_dependents[task]:
            dependent.cancel()

        with self.__condition:
            self.__condition.notify_all()

    def __on_task_canceled(self, task: Task) -> None:
        # Propagate cancellation to all dependent tasks
        for dependent in self.__task_to_dependents[task]:
            dependent.cancel()

        with self.__condition:
            self.__condition.notify_all()

    @property
    def ready_tasks(self) -> Generator[Task, None, None]:
        """
        Yields tasks that are ready to execute (all dependencies completed).
        Blocks until tasks become ready or all tasks are processed.
        Raises SchedulingException if circular dependencies are detected.
        """
        if self.__has_cycles():
            raise SchedulingException("Dependency graph contains circular dependencies")

        with self.__condition:
            while True:
                # Check if there are any remaining pending tasks
                has_pending_tasks = any(task.status == TaskStatus.PENDING for task in self.__task_to_dependencies.keys())
                if not has_pending_tasks:
                    return

                # Find a task whose dependencies are all completed
                ready_tasks = []
                for task, dependencies in self.__task_to_dependencies.items():
                    if task.status != TaskStatus.PENDING:
                        continue

                    # Check if all dependencies are completed (not running, pending, or scheduled)
                    all_dependencies_completed = all(
                        (dependency.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED})
                        for dependency in dependencies
                    )

                    if all_dependencies_completed:
                        ready_tasks.append(task)

                if ready_tasks:
                    for task in ready_tasks:
                        task.mark_as_scheduled()
                        yield task
                else:
                    # No tasks are ready - wait for task state changes
                    self.__condition.wait()

    def __has_cycles(self) -> bool:
        """
        Detects cycles in the dependency graph. Returns True if the graph contains cycles, False otherwise.

        The algorithm works by repeatedly removing nodes with no dependencies
        and updating the dependency counts of their dependents. If we can't
        process all nodes this way, there must be a cycle.
        """
        out_degree = {task: len(dependencies) for task, dependencies in self.__task_to_dependencies.items()}

        # Start with tasks that have no dependencies
        ready_queue = deque(task for task, degree in out_degree.items() if degree == 0)

        processed_count = 0
        while ready_queue:
            task = ready_queue.popleft()
            processed_count += 1

            # Reduce the dependency count for each dependent task
            for dependent in self.__task_to_dependents[task]:
                out_degree[dependent] -= 1
                if out_degree[dependent] == 0:
                    ready_queue.append(dependent)

        # If we couldn't process all tasks, there must be a cycle
        return processed_count != len(self.__task_to_dependencies)