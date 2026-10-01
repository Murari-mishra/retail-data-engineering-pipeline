# Retail Sales ETL Pipeline with Apache Airflow

## Project Overview

This project implements an automated retail sales ETL pipeline using:

- Apache Airflow
- Python
- Pandas
- PostgreSQL
- SQL
- Bash
- Docker
- Docker Compose

The pipeline extracts retail transaction data from CSV,
cleans and transforms the data using Python/Pandas,
loads the processed data into PostgreSQL,
performs SQL-based data validation,
and verifies the processed file using Bash.

## Architecture

CSV
↓
Extract
↓
Transform
↓
PostgreSQL Load
↓
SQL Validation
↓
Bash File Validation

## Technologies

- Python
- Pandas
- Apache Airflow 2.10.2
- PostgreSQL
- Docker
- Docker Compose
- SQL
- Bash

## Airflow Tasks

1. Extract retail sales data
2. Transform and clean the data
3. Load transformed data into PostgreSQL
4. Validate data using SQL
5. Verify transformed file using Bash

## Project Structure

```text
retail-airflow-etl/
│
├── dags/
│   └── retail_etl_dag.py
│
├── scripts/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── sql/
│   └── validate_retail_data.sql
│
├── data/
│   └── retail_orders_data.csv
│
├── config/
│   └── init.sql
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md