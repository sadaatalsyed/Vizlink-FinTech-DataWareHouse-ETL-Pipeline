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
