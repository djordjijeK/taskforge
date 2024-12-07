## TaskForge

TaskForge is a lightweight and flexible library designed to manage and execute tasks that have dependencies on other tasks. 
It ensures that tasks are executed in the correct order, respecting their prerequisites, while also supporting concurrent execution to improve efficiency. 
The library includes features like dependency graph management, automatic failure propagation, and cancellation of dependent tasks when a prerequisite fails or is canceled.

With its simple and clear API, TaskForge allows you to focus on defining tasks and dependencies, without worrying about the complexity of scheduling or execution.
Built with multithreading support, TaskForge enables concurrent execution with customizable thread pools, making it suitable for workflows requiring parallelism.

### Example

The following example demonstrates how TaskForge can be used to create a file processing pipeline. 
In this scenario, each file goes through three stages: reading, processing, and compression. 
Tasks of similar nature share thread pools through tagging, and the framework automatically handles parallel execution while respecting dependencies.

```.python
import time
import logging

# pip3 install . (install taskflow package)
from taskflow import Task, Executor, Scheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s::%(name)s::[%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReadFileTask(Task):
    def __init__(self, filename, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename


    def execute(self):
        logger.info(f"Reading file {self.filename}")
        time.sleep(5)


    def tag(self):
        return "io"  # IO task group


class ProcessFileTask(Task):
    def __init__(self, filename, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename


    def execute(self):
        logger.info(f"Processing file {self.filename}")
        time.sleep(2)


    def tag(self):
        return "cpu" # CPU task group


class CompressTask(Task):
    def __init__(self, filename, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename


    def execute(self):
        logger.info(f"Compressing file {self.filename}")
        time.sleep(1)

    def tag(self):
        return "cpu"  # CPU task group


def process_files(filenames):
    # Create tasks for reading each file
    read_tasks = [ReadFileTask(filename) for filename in filenames]

    # Create processing and compression tasks for each file
    process_tasks = []
    compress_tasks = []

    for read_task in read_tasks:
        # Process task depends on read task
        process_task = ProcessFileTask(read_task.filename, dependencies={read_task})
        process_tasks.append(process_task)

        # Compress task depends on process task
        compress_task = CompressTask(read_task.filename, dependencies={process_task})
        compress_tasks.append(compress_task)

    # Create scheduler with all tasks
    all_tasks = set(read_tasks + process_tasks + compress_tasks)
    scheduler = Scheduler(all_tasks)

    # Create executor with separate thread pools for IO and CPU tasks
    executor = Executor(scheduler, workers_per_tag=2)

    # Run all tasks
    executor.run()

    # Return results from compression tasks
    return [task.result for task in compress_tasks]


if __name__ == "__main__":
    files = ["file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"]
    results = process_files(files)
```

When you run this example, you'll see log messages demonstrating how tasks are executed in parallel while adhering to the correct dependency order. 
The framework ensures that each file's processing task starts only after its reading task is completed, and compression begins only after the processing is finished.