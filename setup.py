from setuptools import setup, find_packages


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="taskforge",
    version="0.1.0",
    author="Djordjije Krivokapic",
    author_email="krivokapic.djordjije@example.com",
    description="A flexible task execution framework with dependency management",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/djordjijeK/taskflow",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
)