# taxi_data_platform

Below is the complete README in the professional style we agreed on: no emojis, no decorative language, and no claims about capabilities that are not yet implemented.

# Taxi Data Platform

A production-oriented data engineering platform built with Azure Databricks, Apache Spark, Structured Streaming, Auto Loader, Delta Lake, Python, and GitHub.

The platform processes NYC Yellow Taxi trip data using a layered Bronze, Silver, and Gold architecture. The implementation focuses on configuration-driven processing, reusable Python components, explicit schema management, thin orchestration notebooks, and clear separation between data processing and operational state.

---

## Overview

The primary objective of this project is to demonstrate how a maintainable Azure Databricks data platform can be structured using software engineering principles.

The platform separates:

* Dataset configuration
* Schema definitions
* Reusable processing logic
* Databricks orchestration
* Data storage
* Pipeline state
* Testing
* Version control

The current implementation processes Yellow Taxi data through the complete:

```text
Source
  |
  v
Bronze
  |
  v
Silver
  |
  v
Gold
```

medallion architecture.

---

## Architecture

```text
                         NYC Yellow Taxi Data
                                  |
                                  | CSV
                                  v
                    +---------------------------+
                    |     Azure Databricks      |
                    |                           |
                    |      Auto Loader          |
                    |  Structured Streaming      |
                    +-------------+-------------+
                                  |
                                  v
                    +---------------------------+
                    |          Bronze           |
                    |                           |
                    | Raw / minimally transformed|
                    | source data                |
                    +-------------+-------------+
                                  |
                                  v
                    +---------------------------+
                    |          Silver           |
                    |                           |
                    | Cleaned                    |
                    | Validated                  |
                    | Standardized               |
                    | Transformed                |
                    +-------------+-------------+
                                  |
                                  v
                    +---------------------------+
                    |           Gold            |
                    |                           |
                    | Curated                   |
                    | Business-oriented         |
                    | Analytics-ready           |
                    +-------------+-------------+
                                  |
                                  v
                         Analytics / Reporting
```

The architecture follows the principle that each layer should have a clearly defined responsibility.

---

## Technology Stack

| Technology                 | Purpose                                  |
| -------------------------- | ---------------------------------------- |
| Azure Databricks           | Data engineering and processing platform |
| Apache Spark               | Distributed data processing              |
| Spark Structured Streaming | Incremental data processing              |
| Databricks Auto Loader     | Incremental file ingestion               |
| Delta Lake                 | Reliable lakehouse storage               |
| Python                     | Reusable application and pipeline logic  |
| YAML                       | Dataset configuration                    |
| GitHub                     | Source control and collaboration         |

---

## Project Structure

```text
taxi_data_platform/
|
├── config/
│   └── datasets/
│       └── yellow_tripdata.yml
|
├── docs/
|
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
|
├── src/
│   ├── config/
│   │   ├── config_loader.py
│   │   └── config_validator.py
│   │
│   ├── schema/
│   │   ├── yellow_trip_schema.py
│   │   └── schema_registry.py
│   │
│   └── ingestion/
│       ├── auto_loader_reader.py
│       └── bronze_writer.py
|
└── tests/
```

The project structure separates configuration, reusable application logic, orchestration notebooks, documentation, and tests.

---

# Data Source

The platform processes NYC Yellow Taxi Trip Record data.

The source files are provided as CSV files and are incrementally ingested using Databricks Auto Loader.

Source location:

```text
/Volumes/taxi/source/source_data/
```

The dataset-specific configuration is maintained in:

```text
config/datasets/yellow_tripdata.yml
```

The configuration defines properties such as:

* Dataset name
* Source format
* Source path
* File pattern
* CSV header configuration
* CSV delimiter
* Target table
* Schema
* Write mode
* Audit configuration

This separates dataset-specific settings from reusable processing logic.

---

# Medallion Architecture

The platform follows the Bronze, Silver, and Gold medallion architecture.

```text
                 +---------+
                 | Source  |
                 +----+----+
                      |
                      v
                 +---------+
                 | Bronze  |
                 +----+----+
                      |
                      v
                 +---------+
                 | Silver  |
                 +----+----+
                      |
                      v
                 +---------+
                 |  Gold   |
                 +---------+
```

Each layer has a distinct purpose.

---

# Bronze Layer

The Bronze layer is responsible for reliable ingestion of source data into Delta Lake while keeping the data as close to the source representation as practical.

The implementation uses:

* Spark Structured Streaming
* Databricks Auto Loader
* Explicit source schema
* Streaming checkpoints
* Separate Auto Loader schema location
* Delta Lake

Bronze target table:

```text
taxi.bronze.yellow_tripdata
```

## Bronze Responsibilities

* Incremental file discovery
* CSV ingestion
* Explicit schema application
* Structured Streaming
* Auto Loader configuration
* Checkpoint management
* Delta table writing
* Minimal transformation

The Bronze layer is intentionally not heavily transformed. Its purpose is to provide a reliable and persistent representation of the ingested source data for downstream processing.

---

# Silver Layer

The Silver layer transforms Bronze data into cleaned, validated, standardized, and reusable datasets.

The Silver layer acts as the boundary between raw ingestion and business-oriented analytical data.

## Silver Responsibilities

* Data cleansing
* Data type standardization
* Null handling
* Invalid-value handling
* Data validation
* Business-rule validation
* Deduplication where required
* Derived attributes
* Standardization for downstream consumers

Silver processing consumes Bronze data and produces curated Delta datasets for the Gold layer.

---

# Gold Layer

The Gold layer contains business-oriented and analytics-ready datasets.

The purpose of the Gold layer is to provide datasets that can be consumed directly by analytical workloads without requiring downstream users to repeatedly perform the same transformation logic.

## Gold Responsibilities

* Business-oriented transformations
* Aggregations
* Analytical metrics
* Revenue analysis
* Trip analysis
* Time-based analysis
* Payment analysis
* Location-oriented analysis

Gold datasets consume validated Silver data rather than directly depending on raw Bronze data.

---

# Configuration-Driven Design

The project separates what the pipeline processes from how the pipeline processes it.

Dataset-specific configuration is maintained under:

```text
config/datasets/
```

For example:

```text
config/datasets/yellow_tripdata.yml
```

The configuration defines dataset-specific properties such as source location, file format, schema, target table, and write behavior.

The general execution model is:

```text
Dataset Configuration
        |
        v
Configuration Loader
        |
        v
Configuration Validator
        |
        v
Reusable Processing Components
        |
        v
Databricks Orchestration Notebook
        |
        v
Bronze / Silver / Gold
```

This design allows additional datasets to reuse the same processing framework without duplicating the complete implementation.

---

# Schema Management

The project uses explicit Spark schemas rather than relying entirely on schema inference.

The Yellow Taxi schema is maintained in:

```text
src/schema/yellow_trip_schema.py
```

Schema registration is handled through:

```text
src/schema/schema_registry.py
```

The schema includes explicit data types for source columns, including:

* Integer fields
* Timestamp fields
* Double fields
* String fields

Explicit schema management provides predictable ingestion behavior and reduces dependency on inferred source types.

---

# Reusable Python Components

Reusable pipeline logic is maintained under `src/`.

```text
src/
|
├── config/
│   ├── config_loader.py
│   └── config_validator.py
|
├── schema/
│   ├── yellow_trip_schema.py
│   └── schema_registry.py
|
└── ingestion/
    ├── auto_loader_reader.py
    └── bronze_writer.py
```

## Configuration Components

### `config_loader.py`

Responsible for loading dataset configuration.

### `config_validator.py`

Validates configuration before it is consumed by the pipeline.

---

## Schema Components

### `yellow_trip_schema.py`

Contains the explicit Spark schema for the Yellow Taxi dataset.

### `schema_registry.py`

Provides centralized schema lookup and management.

---

## Ingestion Components

### `auto_loader_reader.py`

Encapsulates Spark Structured Streaming and Databricks Auto Loader source configuration.

### `bronze_writer.py`

Handles writing streaming data to Delta tables using the configured checkpoint and target table.

---

# Notebook Architecture

Databricks notebooks are organized according to the medallion layers:

```text
notebooks/
├── bronze/
├── silver/
└── gold/
```

The notebooks are intentionally kept thin.

The implementation follows the principle:

```text
Notebook
   |
   +-- Load configuration
   |
   +-- Initialize reusable components
   |
   +-- Orchestrate execution
   |
   +-- Trigger processing
```

rather than placing the complete processing implementation directly inside notebooks.

This separation improves maintainability, reuse, and testability.

---

# Storage Architecture

The project uses Databricks Volumes for source data and pipeline state.

## Source Data

```text
/Volumes/taxi/source/source_data/
```

## Pipeline State

```text
/Volumes/taxi/source/pipeline_state/
```

Pipeline state is maintained separately from source data.

This includes operational artifacts such as:

* Auto Loader checkpoints
* Auto Loader schema information

Separating pipeline state from source data provides a clear operational boundary and avoids mixing runtime state with the source dataset.

---

# Catalog and Table Organization

The Databricks catalog used by the project is:

```text
taxi
```

The Bronze table is:

```text
taxi.bronze.yellow_tripdata
```

The project follows the corresponding medallion organization for downstream layers:

```text
taxi.bronze.*
taxi.silver.*
taxi.gold.*
```

This provides a consistent namespace aligned with the logical data architecture.

---

# Engineering Principles

The project follows several production-oriented engineering principles.

## Configuration over hard-coding

Dataset-specific settings are maintained in configuration files rather than duplicated throughout notebooks and processing code.

## Reusable components

Common functionality is implemented as Python modules under `src/`.

## Thin orchestration

Databricks notebooks are used primarily for orchestration rather than containing the entire application implementation.

## Explicit schemas

Source schemas are defined explicitly to provide predictable data types and ingestion behavior.

## Incremental ingestion

Auto Loader and Structured Streaming are used for incremental file processing.

## Layer separation

Bronze, Silver, and Gold have clearly separated responsibilities.

## Pipeline state isolation

Operational state such as checkpoints and schema information is stored separately from source data.

## Version control

Source code, configuration, notebooks, documentation, and tests are maintained under Git.

---

# Scalability

Although the current implementation focuses on the Yellow Taxi dataset, the architecture is designed to support additional datasets.

The intended extension pattern is:

```text
New Dataset
     |
     v
Add Dataset Configuration
     |
     v
Register Schema
     |
     v
Reuse Processing Components
     |
     v
Create Orchestration
     |
     v
Bronze -> Silver -> Gold
```

The objective is to add datasets through configuration and reusable components rather than duplicating complete pipeline implementations.

---

# Testing

The repository contains a dedicated:

```text
tests/
```

directory for testing reusable project components.

The separation of processing logic from Databricks notebooks makes individual components easier to test independently.

Automated testing will continue to expand as the project progresses toward a more complete CI/CD implementation.

---

# Git and Source Control

The project is maintained in GitHub.

Repository:

```text
taxi_data_platform_git
```

Primary branch:

```text
main
```

The Databricks Git folder provides the development integration between Azure Databricks and GitHub.

The Git repository contains:

* Configuration
* Python source code
* Databricks notebooks
* Tests
* Documentation

Runtime artifacts such as Auto Loader checkpoints and schema state remain outside the Git repository in Databricks Volumes.

---

# Current Implementation Status

## Completed

* [x] Project structure
* [x] Configuration-driven dataset definition
* [x] Configuration loading
* [x] Configuration validation
* [x] Explicit Yellow Taxi schema
* [x] Schema registry
* [x] Auto Loader ingestion
* [x] Spark Structured Streaming ingestion
* [x] Bronze Delta layer
* [x] Silver transformation layer
* [x] Gold analytical layer
* [x] Bronze orchestration
* [x] Silver orchestration
* [x] Gold orchestration
* [x] Reusable Python components
* [x] Separate pipeline-state storage
* [x] Databricks project organization
* [x] GitHub repository
* [x] Databricks Git integration

---

# Engineering Roadmap

The core Bronze, Silver, and Gold platform is implemented.

The next engineering improvements focus on increasing operational maturity rather than changing the core architecture.

* [ ] Expand automated unit and integration testing
* [ ] Expand automated data-quality validation
* [ ] CI/CD implementation
* [ ] Databricks deployment automation
* [ ] Environment-specific configuration
* [ ] Production monitoring and alerting
* [ ] Operational observability
* [ ] Production scheduling and orchestration

These items represent the next stage of production maturity and are intentionally separated from the completed data-processing architecture.

---

# Project Objectives

The project is intended to demonstrate practical data engineering capabilities using Azure Databricks, including:

* Designing a medallion architecture
* Building incremental ingestion pipelines
* Working with Spark Structured Streaming
* Using Databricks Auto Loader
* Building Delta Lake pipelines
* Implementing configuration-driven processing
* Managing explicit schemas
* Designing reusable Python components
* Keeping Databricks notebooks thin
* Separating runtime state from source data
* Applying data transformation and validation
* Organizing a maintainable data engineering repository
* Using GitHub for source control
* Progressing toward automated testing and CI/CD

---

# Design Summary

The platform can be summarized as:

```text
                    Configuration
                         |
                         v
                Reusable Python Code
                         |
                         v
                Databricks Notebooks
                         |
                         v
                +------------------+
                |      Bronze      |
                |                  |
                | Raw ingestion    |
                +--------+---------+
                         |
                         v
                +------------------+
                |      Silver      |
                |                  |
                | Clean / Validate |
                +--------+---------+
                         |
                         v
                +------------------+
                |       Gold       |
                |                  |
                | Analytics-ready  |
                +------------------+
```

The design separates configuration, application logic, orchestration, storage, and operational state while maintaining a clear Bronze-to-Gold processing flow.

---

# Author

**Dilshad Akthar**

Azure Databricks Data Engineering Project

Technologies:

`Azure Databricks` · `Apache Spark` · `Structured Streaming` · `Auto Loader` · `Delta Lake` · `Python` · `GitHub`
