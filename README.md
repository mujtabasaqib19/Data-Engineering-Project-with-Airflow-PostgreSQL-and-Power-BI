# 📊 Data Engineering Project with Airflow, PostgreSQL, and Power BI

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow**, **PostgreSQL**, and **Power BI** for dashboard creation and visualization.

---

## 🚀 Project Overview

- Built an **Airflow pipeline** to automate:
  - Extraction and transformation of **sales**, **product**, and **customer** data.
  - Creation of **fact** and **dimension** tables.
- Loaded the cleaned data into **PostgreSQL** database.
- Connected **Power BI** to the PostgreSQL database to create interactive dashboards.
- Visualized key business metrics such as **Sales by Country**, **Top Selling Products**, and **Customer Distribution**.

---

## 🛠 Project Structure

```bash
.
├── dags/
│   ├── dim_customers.py         # DAG for loading dim_customer
│   ├── dim_products.py          # DAG for loading dim_product
│   ├── extract_fact_sales.py    # DAG for loading fact_sales
│   └── data/
│       ├── CUST_AZ12_UPDATED.csv
│       ├── cust_info.csv
│       ├── LOC_A101.csv
│       ├── prd_info.csv
│       ├── PX_CAT_G1V2.csv
│       └── sales_details.csv
├── logs/                        # Airflow logs
├── plugins/                     # Airflow plugins (if any)
├── .env                          # Environment variables
├── data.dump.ipynb               # Optional data processing notebook
├── docker-compose.yml            # Docker configuration for services
```

---

## ⚙️ Technologies Used

- **Airflow** for workflow orchestration
- **PostgreSQL** for database storage
- **Docker Compose** for containerized setup
- **Power BI** for reporting and visualization

---

## 🔥 Key Features

- **Three automated DAGs**:
  - `dim_customer` → Builds customer dimension.
  - `dim_product` → Builds product dimension.
  - `fact_sales` → Builds sales fact table.
- **Scheduled Daily (@daily)** with retry mechanisms.
- **Dynamic connections** via `.env` files (Airflow reads environment variables).
- **Data validation** before inserting into target tables.
- **Dashboard Examples**:
  - Sales by Country
  - Sales by Product Line
  - Top 10 Customers by Revenue
  - Top 10 Products by Revenue
  - Order Status Funnel

---

## 🐳 How to Run

1. Clone this repository:

```bash
git clone https://github.com/mujtabasaqib19/mujtabasaqib19.git
cd mujtabasaqib19
```

2. Start the services via Docker:

```bash
docker-compose up --build
```

3. Access services:
   - Airflow UI: `http://localhost:8081`
   - PGAdmin (optional): `http://localhost:5050`
   - PostgreSQL Database Host: `localhost:5432`

4. Create an Airflow Admin User:

```bash
docker-compose run airflow-webserver airflow users create -u muji -p 1234 -f Muji -l Admin -r Admin -e muji@example.com
```

5. Trigger the DAGs manually or let them run on schedule.
---

## 📈 Power BI Dashboards

You can connect Power BI to PostgreSQL:
- Host: `localhost`
- Port: `5432`
- Username: `postgres`
- Password: `example`
- Database: `postgres`

**Dashboards Created:**
- Top 10 Products by Sales
- Sales Breakdown by Country
- Sales Amount vs Quantity per Product Line
- Customer Purchase Distribution
- Order Status Funnel (Order Placed → Shipped → Delivered)

---

## 📋 Commands Used During Setup

```bash
# Navigate into the project
cd project-folder

# Run Docker Compose
docker-compose up --build

# Create Airflow Admin User
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Trigger Airflow DAGs
Trigger manually via Airflow UI.
```

---

## 📢 Future Improvements

- Add dbt (data build tool) for better transformations
- Setup email notifications for DAG failure alerts
- Deploy dashboards on Power BI Cloud Service
- Automate incremental loads instead of full refreshes

---

## 🤝 Acknowledgements

- [Apache Airflow](https://airflow.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Power BI](https://powerbi.microsoft.com/)
- [Docker](https://www.docker.com/)

---
