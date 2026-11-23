# OLIST BRAZILIAN E-COMMERCE BIG DATA ETL PIPELINE (APACHE SPARK) 
## Project Overview
This project builds an end-to-end **Big Data ETL (Extract, Transform, Load) pipeline** using **Apache Spark (PySpark)** in **Databricks**. Using the real-world **Olist Brazilian E-Commerce Public Dataset** (nearly 100,000 orders across 9 relational tables), the pipeline cleans messy raw records, combines structured CSV files with semi-structured JSON data, stores the unified dataset in **Apache Parquet** format, and generates business insights using **Spark SQL**.

> **Note on Currency:** All financial values in this project are measured in **Brazilian Reais (`R$` / `BRL`)**, the national currency of Brazil.

---

## End-to-End Pipeline Flowchart

```text
+-----------------------------------------------------------------------+

|  STEP 1: DATA INGESTION (Extract)                                     |
|  - Load 9 raw Olist CSV tables into Databricks Unity Catalog Volume   |
|  - Convert product category translations to JSON (Semi-Structured)    |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+

|  STEP 2: DATA PREPROCESSING (Clean & Filter)                          |
|  - Cast string dates into proper Spark Timestamps                     |
|  - Calculate delivery days & shipping delays                          |
|  - Remove canceled/unavailable orders & zero/negative prices          |
|  - Replace missing (null) product categories with 'unknown'           |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+

|  STEP 3: DATA INTEGRATION & TRANSFORMATION (Prevent Row Duplication)  |
|  - Pre-aggregate Items, Payments, and Reviews to 1 row per order_id   |
|  - Join all cleaned tables into one unified customer-order table      |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+

|  STEP 4: ETL STORAGE (Load to Parquet)                                |
|  - Save unified dataset as compressed Apache Parquet format           |
|  - Register Parquet table as a Spark SQL Temporary View               |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+

|  STEP 5 & 6: SPARK SQL ANALYTICS & VISUALIZATION                      |
|  - Run distributed SQL queries for revenue, payments, & delivery      |
|  - Export visual charts for executive business decision-making        |
+-----------------------------------------------------------------------+
```

---

## Simple Explanation of All 6 Technical Steps

| Step | Stage Name | What We Did (In Simple Words) | Why It Matters |
| :--- | :--- | :--- | :--- |
| **1** | **Dataset Selection & Ingestion** | Loaded ~100,000 e-commerce records across 9 CSV files into Spark, and converted the category lookup table into **JSON** format before reading it back. | Proves the pipeline can handle large multi-table datasets and both structured (CSV) and semi-structured (JSON) formats. |
| **2** | **Data Preprocessing** | Fixed text dates into real timestamps, removed canceled orders and invalid prices, and filled missing product names with `'unknown'`. | Raw business data always has errors; cleaning ensures our final revenue and delivery numbers are accurate. |
| **3** | **Data Integration & Transformation** | Grouped order items, payments, and reviews down to **1 row per order** *before* merging all tables together. | Prevents "row explosion" (a common bug where joining tables multiplies items and accidentally inflates revenue). |
| **4** | **ETL Pipeline (Parquet)** | Saved the final clean, combined table in **Apache Parquet** format instead of CSV. | Parquet compresses data into columns, making big data storage smaller and Spark SQL queries much faster. |
| **5** | **Analytics & Insights (Spark SQL)** | Ran SQL queries on the Parquet data to calculate top categories, monthly sales growth, payment habits, and delivery delays. | Turns raw transaction logs into clear business numbers that managers can act on. |
| **6** | **Visualization** | Converted small aggregated summary tables into visual charts showing revenue leaders, seasonal spikes, and delivery impact. | Visual charts make complex trends immediately understandable for non-technical stakeholders. |

---

## Key Business Findings & Data Breakdown

### 1. Payment Method Distribution (Order Share)
Out of all valid orders analyzed, **Credit Cards** account for three out of every four purchases:

```text
Credit Card  [██████████████████████████████]  75.3%  (75,618 orders)
Boleto       [████████                      ]  19.5%  (19,539 orders)
Voucher      [█                             ]   3.7%  ( 3,745 orders)
Debit Card   [▏                             ]   1.5%  ( 1,515 orders)
```

| Payment Type | Total Orders | Share of Orders (%) | Business Takeaway |
| :--- | :--- | :--- | :--- |
| **Credit Card** | 75,618 | 75.3% | Installment payment options in Brazil drive the majority of sales. |
| **Boleto** (Bank Slip) | 19,539 | 19.5% | Essential cash-based payment alternative for customers without credit cards. |
| **Voucher** | 3,745 | 3.7% | Used for promotional discounts and store credits. |
| **Debit Card** | 1,515 | 1.5% | Smallest payment share across the marketplace. |

---

### 2. Top 5 Revenue-Generating Product Categories

| Rank | Product Category | Approx. Revenue (BRL) | Key Insight |
| :--- | :--- | :--- | :--- |
| **1** | **Health & Beauty** (`health_beauty`) | ~R\$ 1,250,000+ | Highest-earning category on the platform. |
| **2** | **Watches & Gifts** (`watches_gifts`) | ~R\$ 1,200,000 | Strong high-ticket item performance. |
| **3** | **Bed, Bath & Table** (`bed_bath_table`) | ~R\$ 1,040,000 | Consistent household repeat demand. |
| **4** | **Sports & Leisure** (`sports_leisure`) | ~R\$ 980,000 | Major contributor to lifestyle sales. |
| **5** | **Computers & Accessories** (`computers_accessories`) | ~R\$ 910,000 | High average order value per transaction. |

---

### 3. Seasonal Growth & Delivery Performance
* **Black Friday Surge (November 2017):** Monthly revenue grew steadily throughout 2017 and spiked to **~R\$ 1.0 million in November 2017**, maintaining strong monthly volumes between **R\$ 850,000 and R\$ 1.0 million** through mid-2018.
* **Shipping Speed Directly Impacts Customer Ratings (`Correlation: -0.45`):**
  * **Fast Delivery States:** Customers in southeastern hubs like **São Paulo (SP)** receive orders in **~8–9 days** on average and leave high review scores (**~4.25 / 5.0**).
  * **Slow Delivery States:** Distant northern/northeastern states like **Roraima (RR)**, **Alagoas (AL)**, and **Maranhão (MA)** wait **22–30 days** for delivery, dropping average review scores to **~3.85–3.90 / 5.0**.

---

## Repository Structure

```text
olist-spark-etl-pipeline/
│
├── CODE-Brazillian-E-Commerce.ipynb   # Complete Databricks PySpark ETL & Spark SQL notebook
├── README.md                          # Project documentation, architecture, & findings
└── Olist_Big_Data_Project_Report.pdf  # Full 6-chapter technical report
```

---

## Technologies Used
* **Compute Engine:** Apache Spark (PySpark DataFrame API & Spark SQL)
* **Cloud Platform:** Databricks (Serverless Compute & Unity Catalog Volumes)
* **Data Formats:** CSV (Structured), JSON (Semi-Structured), Apache Parquet (Columnar Storage)
* **Visualization:** Python (Matplotlib & Seaborn)
* **Version Control:** GitHub
