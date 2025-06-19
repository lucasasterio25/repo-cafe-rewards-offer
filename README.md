# Cafe Rewards Data Pipeline

# Overview
This project implements a complete data pipeline for the Cafe Rewards Offer Dataset using Databricks and follows the medallion architecture (Raw, Trusted, Refined layers). The goal is to ingest, clean, transform, and analyze marketing data to answer key business questions regarding customer behavior and offer performance.


# Design choices and the technologies used:
For this project, I chose Databricks as the core platform to process, analyze, and manage the data pipeline due to its native support for big data processing with PySpark, seamless integration with Delta Lake, and strong compatibility with Unity Catalog for secure data governance.

To orchestrate the ETL pipeline, I used Databricks Workflows, which provided a simple and scalable way to schedule and monitor each step — from ingesting raw files to transforming and saving datasets in the trusted and refined layers. This choice eliminated the need for an external orchestration tool like Azure Data Factory, reducing complexity, improving maintainability and cost reduction.

The data was stored using Unity Catalog, following a medallion architecture pattern:
Raw Layer: stores the ingested source files from Kaggle (CSV).
Trusted Layer (Silver): contains cleaned, typed, and enriched datasets.
Refined Layer (Gold): includes dimension tables, a fact table, and aggregated analytical outputs used to answer the business questions.
All data was saved in Delta format, enabling fast, reliable querying and support for time travel and ACID transactions. This architecture allows for scalability, modularity, and clear separation of concerns across the pipeline.

# Technologies Used
Databricks: Unified platform for data engineering and analytics
Databricks Workflows: Used for ETL orchestration
PySpark: Data processing and transformations
Delta Lake: Reliable storage format with ACID transactions
Delta Lake volume: Upload files with security
Unity Catalog + Volumes: Governed and secure data storage


# Design & Architecture Decisions
PySpark was selected for distributed processing and performance on large datasets.
Delta Lake ensures ACID transactions, time travel, and better reliability for data pipelines.
Layered Lakehouse Architecture (Raw → Trusted → Refined) follows best practices in modern data engineering.
Azure Databricks provides native integration with Azure Data Lake and simplified Spark management.


# Medallion Architecture
Raw Layer (Bronze)
Ingested raw files from Kaggle into Unity Catalog Volumes using API

Trusted Layer (Silver)
Cleaned and normalized datasets:
Cast data types
Removed nulls and format data
Parsed dates and created useful derived columns

Refined Layer (Gold)
Generated:
fact_events
dim_customer
dim_offer


# Author
Lucas Sousa
