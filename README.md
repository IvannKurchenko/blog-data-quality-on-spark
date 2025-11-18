# Data Quality On Spark

## Introduction

This repository contains the source code accompanying the blog series "Data Quality On Spark". It demonstrates data quality practices using Apache Spark, including connecting to databases and validating data.

## Prerequisites

- Python (version 3.8 or higher)
- Apache Spark
- uv (Python package installer and resolver)

## Getting Started

To set up and run the project, follow these steps:

1. **Set up the virtual environment**: Run `make setup` to create a Python virtual environment using uv.
2. **Install dependencies**: Run `make install` to install the required Python packages.
3. **Download MariaDB driver**: Run `make download-maria-db-driver` to download the MariaDB Java client driver, which is necessary for connecting to the test dataset database.
4. **Download FAA registry**: Run `make download_faa_registry` to download and unzip the FAA registry data, used for data validation purposes.

After completing these steps, you can start working with the repository's code.