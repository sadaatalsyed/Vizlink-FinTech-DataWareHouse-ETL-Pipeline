# Vizlink Data Warehouse — ETL Pipeline

An incremental ETL pipeline that moves data from the **Vizlink OLTP** database (`vizlink-live`) into the **Vizlink Data Warehouse** (`VDWH_Auto`) using Python, pandas, and pyodbc.

---

## Architecture

```
vizlink-live (SQL Server OLTP)
        │
        │  pyodbc / pandas
        ▼
etl_pipeline.py
        │
        │  SQLAlchemy / pyodbc
        ▼
VDWH_Auto (SQL Server OLAP / Star Schema)
```

### Tables Loaded

| Layer | Table |
|-------|-------|
| Dimension | Dim_DistCenter |
| Dimension | Dim_Users |
| Dimension | Dim_DMs |
| Dimension | Dim_Vizshops |
| Dimension | Dim_DistributorShop |
| Fact | Fact_ShopOrders |
| Fact | Fact_ShopOrderTransactions |

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/sadaatalsyed/vizlink-data-warehouse-etl.git
cd vizlink-data-warehouse-etl
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure credentials
```bash
cp config/config.env.example config/config.env
# Edit config/config.env with your server names
```

> **Important:** `config/config.env` is in `.gitignore` — never commit it.

### 4. Run the pipeline
```bash
python etl/etl_pipeline.py
```

Logs are written to `etl_pipeline.log` and printed to the console.

---


---




