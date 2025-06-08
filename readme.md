# 🌍 Global Retail Sales Analytics — End-to-End Data Engineering Project

This project demonstrates a complete end-to-end data engineering pipeline for global retail sales analytics. It simulates real-world big data processing and analysis, utilizing modern data engineering tools and practices.

---

## 🧰 Tech Stack

- **Orchestration**: [Kestra](https://kestra.io/)
- **Processing & Transformation**: [Apache Spark](https://spark.apache.org/), [Python](https://www.python.org/)
- **Data Lake**: [MinIO](https://min.io/)
- **Data Warehouse**: [Snowflake](https://www.snowflake.com/)
- **RDBMS**: [PostgreSQL](https://www.postgresql.org/)
- **Data Modeling**: [dbt](https://www.getdbt.com/)
- **Visualization**: [Metabase](https://www.metabase.com/)

---

## 🗂️ Project Structure
.
pipeline_global_sales/
├── data/ # Raw input datasets
│ └── global_fashion_dbt/ # dbt project directory
│ └── ... # dbt models & staging files
├── logs/ # Logging (e.g., from Spark jobs)
├── postgres_dbt/ # PostgreSQL setup & dbt connection
├── postgres-db/ # PostgreSQL service (Dockerized)
├── flows/ # Kestra YAML workflows
├── docker-compose.yml # Service orchestration for all tools
├── download_data.sh # Script to fetch or move datasets
├── Read CSV with Spark.ipynb # Spark ETL notebook (manual testing)
├── readme.md # Project documentation

---

## 🔁 Workflow Overview

### 1. **Raw Data Ingestion to MinIO (Data Lake)**
- All raw data (transactions 1-11, customers 1-5, and reference datasets like store, employee, product, etc.) are uploaded into a data lake on MinIO in their raw format.
- The datasets are large, requiring big data tools for processing.

### 2. **Orchestration with Kestra**
- Kestra is used to orchestrate the entire workflow including:
  - One-time backfill and regular batch scheduling.
  - Monitoring and logging for production-grade reliability.

### 3. **Spark Processing**
- Spark reads data directly from MinIO.
- Missing values are handled using Spark transformations.
- Aggregation and `groupBy` operations are applied for big data summarization.
- Raw cleaned data is written back to MinIO in a structured format (e.g., Parquet).

### 4. **Data Loading**
- Cleaned data is loaded into two destinations:
  - **PostgreSQL** (for analytics engineers and dbt modeling).
  - **Snowflake** (for cloud-native querying and scalability).

### 5. **Analytics Engineering**
- **dbt** is used to build models (e.g., `fact_sales`) on PostgreSQL using:
  - `stg_transactions_retail.sql`
  - `stg_customers_retail.sql`
  - `store`, `employee`, `product`, and `discount` tables.
- **Metabase** connects to PostgreSQL to visualize KPIs and trends for business users.

### 6. **Advanced Cloud Analytics**
- Additional queries are run in Snowflake for multi-currency analysis, global views, and large-scale aggregation.

---

## 📊 Dashboard

The visualization dashboard below was created using **Metabase** and showcases:

- Total Sales Per Month
- Sales by Payment Method
- Global Sales Volume by Country
- Top-Selling Product Categories
- Sales by Age Group and Gender
- Multi-Currency Analysis by Country

![Global Sales Dashboard](https://user-images.githubusercontent.com/12345678/171234567-abc12345-xyz890.jpg)

---

## 📌 Key Highlights

- Handles large-scale retail data efficiently using Apache Spark.
- Combines batch orchestration, data lake, and cloud warehouse paradigms.
- Implements analytics engineering best practices with dbt and PostgreSQL.
- Enables business-friendly insights with Metabase dashboards.
- Supports cloud scalability and flexible querying with Snowflake.

---

## 🚀 How to Run

1. Upload raw data to MinIO bucket.
2. Run Kestra flows for backfill or schedule.
3. Execute Spark jobs (locally or cluster).
4. Load processed data into PostgreSQL and Snowflake.
5. Run `dbt run` to build models.
6. Explore and visualize using Metabase.

---

## 📬 Contact

For questions or feedback, please reach out to **nicholasanayaputraa@gmail.com** — aspiring Data Engineer passionate about building scalable data solutions.
