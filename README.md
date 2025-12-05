
# Data Quality On Spark

## Introduction

This repository contains the source code accompanying the blog series "Data Quality On Spark". It demonstrates data quality practices using Apache Spark, including connecting to databases and validating data.
The codebase is organized into isolated packages for each data quality tool, allowing independent setup and execution.

## Repository Structure

- `great_expectations/`: Isolated package for Great Expectations data quality evaluations
- `soda/`: Isolated package for Soda data quality evaluations
- `dqx/`: Shared common utilities and constants
- `blog/`: Blog post content

Each package has its own virtual environment, dependencies, and setup.

## Prerequisites

- Python (version 3.11 or higher)
- Apache Spark
- uv (Python package installer and resolver)

## Getting Started

### Shared Setup (Root Level)

1. **Download MariaDB driver**: Run `make download-maria-db-driver` to download the MariaDB Java client driver.
2. **Download FAA registry**: Run `make download-faa` to download and unzip the FAA registry data.

### Per-Package Setup

For each data quality tool, navigate to its directory and follow these steps:

1. **Set up virtual environment**: Run `make setup` to create a Python virtual environment.
2. **Install dependencies**: Run `make install` to install the required packages.
3. **Download data (if needed)**: Run `make download-faa` and `make download-maria-db-driver` if not done at root.

Example for Great Expectations:
```bash
cd great_expectations
make setup
make install
make download-faa
make download-maria-db-driver
python src/evaluation_great_expectations.py
```

Similarly, for Soda:
```bash
cd soda
make setup
make install
# ... same data downloads
python src/evaluation_soda.py
```

## Running Evaluations

Each package can be run independently with its own virtual environment. The evaluations connect to the same test datasets but use different data quality frameworks.