# News Data Extraction and Visualization Project

This project is designed to extract, process, and visualize news data from various sources, providing valuable insights into daily news trends. By leveraging an ETL (Extract, Transform, Load) pipeline and interactive visualization tools, this project aims to streamline the process of accessing and comprehending news data.

## Table of Contents
- [Project Overview](#project-overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Database Management](#database-management)
- [Visualization](#visualization)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

In today's information-rich environment, understanding and analyzing daily news data is challenging. This project addresses this challenge by:

- Establishing an automated ETL data pipeline for data extraction, cleaning, transformation, and loading.
- Ensuring data accuracy, optimal storage, and efficiency in managing incremental updates.
- Creating interactive visualizations to present insights into trending topics, sentiment analysis, and regional news coverage.

## Getting Started

### Prerequisites

To run this project, you will need:

- [Python](https://www.python.org/downloads/)
- [Apache Airflow](https://airflow.apache.org/docs/stable/start.html) for task automation
- [PostgreSQL](https://www.postgresql.org/download/) for data storage
- Required Python libraries (install using `pip install -r requirements.txt`)

### Project Structure
`data_extraction/`: Contains code for data extraction from NewsAPI.
`data_cleaning/`: Includes scripts for data cleaning and normalization.
`database/`: Manages PostgreSQL database setup and schema design.
`visualization/`: Houses code for generating interactive visualizations.
`airflow/`: Defines Apache Airflow DAGs for task automation.

### Data Pipeline
This project follows a structured ETL (Extract, Transform, Load) process:

- Extract: News data is obtained from NewsAPI and processed for cleaning and transformation.
- Transform: Data is cleaned, normalized, and structured into a star schema for efficient querying.
- Load: Cleaned data is loaded into a PostgreSQL database with primary and foreign key relations.

### Visualization
Interactive visualizations are generated to depict daily and overall word clouds.
News source statistics are presented, showcasing the number of articles each source has.
