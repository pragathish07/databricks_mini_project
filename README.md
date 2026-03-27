# Databricks Mini Project – P&L AND WORFORCE ANALYSIS

## Project Overview

This project implements a complete Medallion Architecture data lakehouse solution for financial and HR analytics. The project processes raw CSV files stored in S3, transforms them through bronze and silver layers, and creates a dimensional data model with KPIs in the gold layer.


### Architecture: Bronze → Silver → Gold (Medallion/Delta Lake Architecture)
### Catalog: mini_project
### Storage: AWS S3 (s3://db-lakehouse-s3/lakehouse/)
### Compute: Serverless SQL Warehouse


## Layer 1: Bronze Layer (Raw Data Ingestion)

Ingest raw CSV files from S3 into Delta tables with minimal transformation.

### Schema: mini_project.bronze
### Notebook: nb_bronze_ingestion

Process:
1.	Creates bronze schema with managed location in S3
2.	Reads ingestion configuration from /Volumes/mini_project/bronze/config_volume/ingestion_metadata.json
3.	Iterates through configuration to create tables from CSV files using read_files()
4.	Maintains raw data types with schema inference

### Source Tables Created:
•	employee - Employee master data
•	company - Company information
•	departments - Department structure
•	general_ledgers - General ledger transactions
•	accounts - Chart of accounts
•	payroll - Payroll transactions


## Layer 2: Silver Layer (Cleaned & Standardized)

Clean, standardize, and validate data from bronze layer.
Schema: mini_project.silver

### Notebooks:
•	nb_silver_schema - Creates silver schema
•	nb_silver - Performs data transformations


## Layer 3: Gold Layer (Analytics-Ready)

Create dimensional model and business KPIs for analytics and reporting.
Schema: mini_project.gold


## Part A: Dimensional Data Mart

### Notebook: nb_gold_datamart

### Dimension Tables:

1. dim_date
2. dim_company
3. dim_department
4. dim_account
5. dim_employee
6. dim_position

### Fact Tables:

1. fact_general_ledger
2. fact_payroll



## Part B: Data Cube for KPI Aggregation

### Notebook: nb_gold_data_cube

### datacube_kpi
•	Purpose: Pre-aggregated monthly metrics at company and department level
•	Grain: One row per company, department, year, month

### Measures:
•	Revenue Metrics:
o	gl_revenue
o	gl_cogs
o	gl_total_expenses
o	gl_transaction_count
•	Payroll Metrics:
o	employee_count
o	payroll_total_compensation
o	payroll_overtime
o	payroll_bonus
•	Calculated KPIs:
o	gross_profit (revenue - cogs)
o	net_profit (revenue - total_expenses)
o	gross_margin_pct
o	net_margin_pct
o	revenue_per_employee
o	cost_per_employee

### Source: Joins gl_monthly and payroll_monthly CTEs


## Part C: Business KPIs

### Notebook: nb_gold_kpi

Creates 10 specific KPI tables for business reporting:

KPI 1: kpi_monthly_revenue

KPI 2: kpi_cost_of_sales

KPI 3: kpi_gross_profit_margin

KPI 4: kpi_operating_expenses

KPI 5: kpi_avg_compensation_by_position

KPI 6: kpi_net_profit

KPI 7: kpi_overtime_bonus_analysis

KPI 8: kpi_cost_per_department

KPI 9: kpi_headcount_by_department

KPI 10: kpi_payroll_revenue_ratio




## Data Flow Summary

S3 CSV Files
    ↓
[Bronze Layer - Raw Ingestion]
    ↓ (Date parsing, type casting, cleaning)
[Silver Layer - Standardized]
    ↓ (Dimensional modeling)
[Gold Layer - Datamart]
    ├── Dimensions (date, company, department, account, employee, position)
    └── Facts (general_ledger, payroll)
        ↓ (Monthly aggregation)
    [Datacube KPI]
        ↓ (Business-specific calculations)
    [10 KPI Tables]

    

## Key Technical Patterns
1.	Medallion Architecture: Bronze (raw) → Silver (cleansed) → Gold (curated)
2.	Delta Lake: All tables stored as Delta format
3.	Dimensional Modeling: Star schema with facts and dimensions
4.	Pre-Aggregation: Datacube for performance optimization
5.	Metadata-Driven: JSON configuration for ingestion
6.	Date Standardization: Consistent 'dd-MM-yyyy HH:mm' parsing
7.	NULLIF Protection: Prevents division by zero errors
8.	Unity Catalog: Three-level namespace (catalog.schema.table)
9.	Presentation Dashboard : created KPI dashboards

   
## Business Analytics Use Cases
•	Financial Reporting: P&L, revenue trends, margin analysis
•	HR Analytics: Headcount, compensation benchmarking, payroll ratios
•	Cost Management: Department-level cost tracking, efficiency metrics
•	Performance Monitoring: MoM growth, profitability trends
•	Workforce Planning: Headcount distribution, compensation analysis


