# 🌍 Global Retail Sales Analytics — End-to-End Data Engineering Project

This project demonstrates a complete end-to-end data engineering pipeline for global retail sales analytics. It simulates real-world big data processing and analysis, utilizing modern data engineering tools and practices.

---

## 🧠 Business Understanding

### 🧩 Problem Statements
- How to integrate various global retail data sources into a reliable and scalable pipeline system?
- How to understand sales patterns based on location, time, product category, and discount impact?
- How to present sales information in an interactive dashboard to support business decision-making?

### 🎯 Goals
- Build an end-to-end ETL pipeline that processes data from data lake to data warehouse using Kestra, dbt, and PostgreSQL/Snowflake.
- Provide interactive and informative visualizations through Metabase to help stakeholders understand business performance.
- Deliver insights based on data analysis related to product sales, discount effectiveness, and branch performance.

### ✅ Solution Statements
- The pipeline is built using Kestra for ETL orchestration, Apache Spark for initial transformation, and dbt for data modeling.
- Data is stored in PostgreSQL/Snowflake and then visualized using Metabase as an analytics dashboard.
- Analysis is performed on fact and dimension models (product, customer, time, store) to identify trends, anomalies, and business optimization opportunities.

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
```
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
```
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

<img src="https://github.com/user-attachments/assets/b43c9f26-a453-4e02-b32e-07cab95c8d7b" width="600" />

---

## 📊 Evaluation & Results

### Pipeline Performance
- The ETL pipeline orchestrated with Kestra demonstrated high reliability and scalability, successfully handling increasing data volume from multiple global retail sources without failure.
- Apache Spark efficiently processed and transformed large datasets, reducing data latency and enabling near real-time analytics.
- dbt models provided consistent and well-documented data transformations, ensuring data quality and maintainability in the data warehouse.

### Data Quality & Accuracy
- Data validation checks embedded within the pipeline detected and minimized errors such as missing values, duplicates, and inconsistent formats.
- Cross-validation against source systems confirmed the accuracy of aggregated sales metrics and dimension data.

### Business Insights
- Sales pattern analysis revealed key trends, including peak sales periods by location and product category, and the measurable impact of discount campaigns on sales uplift.
- Branch performance reports highlighted high-performing stores and identified underperforming locations for targeted business interventions.

### Dashboard Effectiveness
- Metabase dashboards received positive feedback from stakeholders for their clarity, interactivity, and real-time updates.
- The dashboards enabled faster decision-making and strategic planning by providing comprehensive sales insights in an accessible format.

### Scalability & Future Improvements
- The modular pipeline architecture supports easy addition of new data sources and scaling to higher data volumes.
- Future improvements include incorporating machine learning models for predictive sales analytics and automated anomaly detection to proactively address data or business issues.

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
