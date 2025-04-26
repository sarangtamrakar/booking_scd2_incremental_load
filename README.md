# Project Title: Travel Bookings Data Ingestion Pipeline

## Description

This project provides an **end-to-end data ingestion pipeline** for processing **travel bookings** and **customer data**. The pipeline utilizes **Databricks**, **PySpark**, **Delta Lake**, and **PyDeequ** to:

- Perform **data quality checks**.
- Aggregate **booking data** (e.g., total bookings, total amount).
- Maintain **historical records** using **Slowly Changing Dimensions (SCD2)**.
- Write the processed data into **Delta tables** on **AWS S3** for efficient querying and reporting.

## Key Features

- **Data Quality Checks**: Comprehensive checks are performed using the **PyDeequ** library, ensuring that the data meets quality standards.
- **Aggregations**: Booking data is aggregated (e.g., total bookings, total amount) and written to the **`bookings_fact`** Delta table.
- **SCD2 (Slowly Changing Dimensions)**: Historical updates for the **`customers_dim`** table are handled using SCD2.
- **Partitioned Delta Tables**: Data is written to partitioned Delta tables, ensuring scalability and efficient querying.

## Technologies Used

- **Databricks**: A cloud platform for building and running data engineering workflows.
- **PySpark**: A framework for large-scale data processing in distributed environments.
- **Delta Lake**: A storage layer built on top of **Apache Spark** for managing large-scale data lakes with ACID transactions.
- **PyDeequ**: A library built on top of **Deequ** for performing data quality checks in big data environments.
- **AWS S3**: Object storage service used to store and manage data.

## Prerequisites

- **Databricks Workspace**: A Databricks workspace is required to run the notebooks and jobs.
- **AWS S3**: An **S3 bucket** in AWS to store and read the Delta tables.
- **Delta Lake**: Ensure that the **Delta Lake** library is available in your Databricks environment.

## Setup and Installation

Follow the steps below to get the project up and running:

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/sarangtamrakar/booking_scd2_incremental_load.git
cd booking_scd2_incremental_load

```




