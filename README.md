# Good Eats

The *Good Food Purchasing* dataset, published by NYC OpenData, provides detailed, item-level records of food procurement by New York City agencies. The data includes descriptions of individual food items, quantities purchased, pricing information, purchase weights and the agencies responsible for each transaction, offering a rare and granular view into municipal food sourcing patterns. Collected as part of the City’s Good Food Purchasing program and last updated in September 2023, the dataset is publicly accessible through multiple formats (CSV, JSON, XML and RDF), enabling transparent analysis of government food purchasing activity and supporting research into supply chains, public nutrition policy and institutional food systems.

## Creating a Development Environment and Installing Dependencies

Once the repository has been refactored to reflect the new project name, development should proceed within an isolated Python environment. An isolated environment prevents conflicts with system-level packages and ensures that dependencies specified in the `pyproject.toml` are installed in a controlled and reproducible manner. Several environment managers may be used for this purpose, and the choice depends on the user’s preferred workflow.

### Using `venv`

Python’s built-in `venv` module provides a minimal, lightweight method for creating an isolated environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install poetry
```

After activation, installing Poetry inside the environment ensures that all subsequent dependency management remains isolated within the project.

### Using Conda

Users who prefer Conda may create an environment with a specified Python version and then install Poetry within it:

```bash
conda create -n good-eats python=3.11
conda activate good-eats
pip install poetry
```

### Installing Project Dependencies

After activating the environment—whether created with `venv` or Conda—dependencies for the new project, including development dependencies, may be installed with a single command:

```bash
poetry install --with dev
```

Poetry will resolve the dependency graph specified in the `pyproject.toml` and populate the environment accordingly. Once completed, the environment is fully prepared for development, testing and further extension of the project.

To download the Good Food Purchasing dataset itself,

```bash
make data
```

## Java Requirement for PySpark

Portions of the test suite rely on PySpark for validating the parity of data processing between pandas and Spark. Because PySpark launches a Java Virtual Machine (JVM) internally, a functioning Java installation is required. Without Java, attempts to construct a `SparkSession` will fail with errors such as:

> *“Unable to locate a Java Runtime… Java gateway process exited…”*

To ensure PySpark can initialize correctly, install a Java Development Kit (JDK) and make it available in the active shell environment.

### Installing Java on macOS

The recommended approach is to install a modern, OpenJDK-compatible distribution. When using Homebrew, a compatible JDK (Temurin 17) may be installed as follows:

```bash
brew install temurin@17
```

After installation, confirm that Java is available on the PATH:

```bash
java -version
```

macOS users should also define the `JAVA_HOME` environment variable so that PySpark and related tools consistently locate the runtime. This may be done using Apple’s Java home utility:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Adding this line to `~/.zshrc` or the relevant shell profile ensures it is configured for all future sessions.

### Installing Java on Ubuntu

Ubuntu users may install OpenJDK from the default package repository. A suitable version for PySpark is OpenJDK 17:

```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

Verify the installation:

```bash
java -version
```

If necessary, set the `JAVA_HOME` variable explicitly:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

This variable may be added to `~/.bashrc` or `~/.profile` to ensure persistence across sessions.

### Verifying PySpark Integration

Once Java is installed and discoverable, the following command should succeed inside the project’s Python environment:

```bash
python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[*]').getOrCreate().stop()"
```

If this command completes without raising an exception, the environment is correctly configured for executing the PySpark-based tests included in the `good_eats` test suite.

## Project Structure

The `good-eats` repository is organized as a Python package with supporting directories for data, notebooks, and automation:

```
good-eats/
    good_eats/               # Main Python package
        __init__.py
        _version.py
        directories.py
        tests/
            test_directories.py
            test_example.py
            test_version.py
            test_data/
                README.md
    data/
        good_eats.csv
        README.md
    notebooks/
        README.md
    Makefile
    pyproject.toml
    poetry.lock
    LICENSE
    README.md
    .github/workflows/
        python-package.yml
```

## Directory Resolution Utilities

The `directories.py` module centralizes the logic for resolving absolute paths to important project locations. Instead of manually constructing file paths or relying on the current working directory, it provides functions that return canonical paths to the package, the project root, the `data/` directory, and the test directories. Each function may return the directory itself or a full file path if a filename is supplied.

### Example Usage

The following example demonstrates how to load the Good Eats dataset into a pandas DataFrame using the path resolution utilities:

```python
import pandas as pd
from good_eats import directories

# Resolve path to the dataset and load it into a DataFrame
filepath = directories.data("good_eats.csv")
df = pd.read_csv(filepath)

df.head()  # inspect the first few rows
```

## Makefile-Based Workflow

The repository includes a `Makefile` intended to streamline routine development tasks. All commands are executed through Poetry, ensuring that tests, formatting and linting run inside the project’s configured environment. The Makefile defines targets for running the test suite, formatting the codebase and checking code style.

Once the development environment has been created and dependencies installed, the following commands may be issued from the project root:

| Command       | Action Performed                                                   |
|---------------|--------------------------------------------------------------------|
| `make test`   | Runs the full test suite with Pytest using the Poetry environment. |
| `make format` | Applies Black formatting across the project source tree.           |
| `make lint`   | Executes Flake8 to perform static analysis and style checking.     |
| `make check`  | Runs formatting, linting and tests in sequence.                    |

These commands provide a concise interface for routine quality assurance. In practice, `make check` is the most comprehensive option, as it formats the code, evaluates style compliance and executes tests in a single step.

