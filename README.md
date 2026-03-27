
# Databricks Mini Project – P\&L AND WORKFORCE ANALYSIS

## Project Overview

This project implements a complete Medallion Architecture data lakehouse solution for financial and HR analytics. The project processes raw CSV files stored in S3, transforms them through bronze and silver layers, and creates a dimensional data model with KPIs in the gold layer.

***

### **Architecture:** Bronze → Silver → Gold (Medallion/Delta Lake Architecture)

### **Catalog:** mini\_project

### **Storage:** AWS S3 (`s3://db-lakehouse-s3/lakehouse/`)

### **Compute:** Serverless SQL Warehouse

***

## **Layer 1: Bronze Layer (Raw Data Ingestion)**

Ingest raw CSV files from S3 into Delta tables with minimal transformation.

### **Schema:** `mini_project.bronze`

### **Notebook:** `nb_bronze_ingestion`

### **Process:**

1.  Creates bronze schema with managed location in S3
2.  Reads ingestion configuration from `/Volumes/mini_project/bronze/config_volume/ingestion_metadata.json`
3.  Iterates through configuration to create tables from CSV files using `read_files()`
4.  Maintains raw data types with schema inference

### **Source Tables Created:**

*   employee - Employee master data
*   company - Company information
*   departments - Department structure
*   general\_ledgers - General ledger transactions
*   accounts - Chart of accounts
*   payroll - Payroll transactions

***

## **Layer 2: Silver Layer (Cleaned & Standardized)**

Clean, standardize, and validate data from bronze layer.

### **Schema:** `mini_project.silver`

### **Notebooks:**

*   `nb_silver_schema` - Creates silver schema
*   `nb_silver` - Performs data transformations

***

## **Layer 3: Gold Layer (Analytics-Ready)**

Create dimensional model and business KPIs for analytics and reporting.

### **Schema:** `mini_project.gold`

***

## **Part A: Dimensional Data Mart**

### **Notebook:** `nb_gold_datamart`

### **Dimension Tables:**

1.  dim\_date
2.  dim\_company
3.  dim\_department
4.  dim\_account
5.  dim\_employee
6.  dim\_position

### **Fact Tables:**

1.  fact\_general\_ledger
2.  fact\_payroll

***

## **Part B: Data Cube for KPI Aggregation**

### **Notebook:** `nb_gold_data_cube`

### **Table:** `datacube_kpi`

**Purpose:** Pre-aggregated monthly metrics at company and department level  
**Grain:** One row per company, department, year, month

### **Measures:**

#### **Revenue Metrics**

*   gl\_revenue
*   gl\_cogs
*   gl\_total\_expenses
*   gl\_transaction\_count

#### **Payroll Metrics**

*   employee\_count
*   payroll\_total\_compensation
*   payroll\_overtime
*   payroll\_bonus

#### **Calculated KPIs**

*   gross\_profit (revenue - cogs)
*   net\_profit (revenue - total\_expenses)
*   gross\_margin\_pct
*   net\_margin\_pct
*   revenue\_per\_employee
*   cost\_per\_employee

**Source:** Joins `gl_monthly` and `payroll_monthly` CTEs

***

## **Part C: Business KPIs**

### **Notebook:** `nb_gold_kpi`

Creates 10 specific KPI tables for business reporting:

1.  **kpi\_monthly\_revenue**
2.  **kpi\_cost\_of\_sales**
3.  **kpi\_gross\_profit\_margin**
4.  **kpi\_operating\_expenses**
5.  **kpi\_avg\_compensation\_by\_position**
6.  **kpi\_net\_profit**
7.  **kpi\_overtime\_bonus\_analysis**
8.  **kpi\_cost\_per\_department**
9.  **kpi\_headcount\_by\_department**
10. **kpi\_payroll\_revenue\_ratio**

***

## **Data Flow Summary**

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

***

## **Key Technical Patterns**

1.  Medallion Architecture: Bronze (raw) → Silver (cleansed) → Gold (curated)
2.  Delta Lake: All tables stored as Delta format
3.  Dimensional Modeling: Star schema with facts and dimensions
4.  Pre-Aggregation: Datacube for performance optimization
5.  Metadata-Driven: JSON configuration for ingestion
6.  Date Standardization: Consistent `'dd-MM-yyyy HH:mm'` parsing
7.  NULLIF Protection: Prevents division by zero errors
8.  Unity Catalog: Three-level namespace (catalog.schema.table)
9.  Presentation Dashboard: created KPI dashboards

***

## **Business Analytics Use Cases**

*   Financial Reporting: P\&L, revenue trends, margin analysis
*   HR Analytics: Headcount, compensation benchmarking, payroll ratios
*   Cost Management: Department-level cost tracking, efficiency metrics
*   Performance Monitoring: MoM growth, profitability trends
*   Workforce Planning: Headcount distribution, compensation analysis

***



