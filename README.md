# 706_Airflow_Demo

**What the DAG does:**
1. **Fetch data**
   - Generates two fake datasets (`sales.csv`, `customers.csv`) using the `Faker` library.
2. **Transform data**
   - Cleans and standardizes the CSVs (email, phone number, etc.).
3. **Merge datasets**
   - Combines sales and customer information into one dataset.
4. **Load to PostgreSQL**
   - Writes the merged data to a database table for SQL analysis.
5. **Analyze**
   - Runs a simple aggregation query and saves results as CSV and chart image.
6. **Clean up**
   - Deletes intermediate CSV files (keeps final output only).

---

## 📊 Datasets Summary

**Sales Data (`sales.csv`)**
- 100 transaction records  
- Fields: `transaction_id`, `customer_id`, `product_category`, `amount`, `quantity`, `transaction_date`, `payment_method`

**Customer Data (`customers.csv`)**
- 100 customer records  
- Fields: `customer_id`, `firstname`, `lastname`, `email`, `phone`, `city`, `country`

> ⚠️ *All data is randomly generated using Faker (GenAI simulation) and not real.*

---

## ⚙️ Prerequisites

Before running this pipeline:
- Docker Desktop is running  
- Apache Airflow environment is set up (via `docker-compose.yaml`)
- PostgreSQL connection configured in Airflow (`Conn Id = Postgres`)

Recommended project structure:
airflow/
├── dags/
│ └── pipeline_v2.py
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── data/
├── raw/
├── clean/
└── output/

## 📁 Results
After a successful run:
Final merged and analyzed outputs are stored in:
/opt/airflow/data/output/
(or data/output/ in your local project)
The PostgreSQL table (week8_demo.employees) can be queried for further analysis.

## 📈 Example Output

Airflow DAG Graph View  
![Airflow DAG Graph](/Users/otting/Desktop/1.png)



