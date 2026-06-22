"""
Vizlink Data Warehouse - ETL Pipeline
======================================
OLTP Source  : vizlink-live (SQL Server)
OLAP Dest    : VDWH_Auto   (SQL Server)

Improvements over original notebook:
  - Credentials moved to config/config.env (not hardcoded)
  - NOT IN replaced with LEFT JOIN / NOT EXISTS for large tables
  - One reusable load_incremental() helper — no duplicate code
  - Error handling with rollback on each block
  - Logging to file + console
  - Single connection opened per block (not per query)
"""

import logging
import os
import urllib

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config  (reads from config/config.env)
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/config.env"))

SOURCE_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SOURCE_SERVER')};"
    f"DATABASE={os.getenv('SOURCE_DB', 'vizlink-live')};"
    f"Trusted_Connection={os.getenv('SOURCE_TRUSTED', 'yes')};"
)

DEST_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DEST_SERVER')};"
    f"DATABASE={os.getenv('DEST_DB', 'VDWH_Auto')};"
    f"Trusted_Connection={os.getenv('DEST_TRUSTED', 'yes')};"
)


def get_engine():
    params = urllib.parse.quote_plus(DEST_CONN_STR)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


# ---------------------------------------------------------------------------
# Reusable helper
# ---------------------------------------------------------------------------
def load_incremental(query: str, dest_table: str, engine) -> int:
    """
    Run *query* against the source DB, append results to *dest_table*.
    Returns number of rows inserted.
    """
    try:
        with pyodbc.connect(SOURCE_CONN_STR) as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            log.info("%-35s  no new rows", dest_table)
            return 0

        df.to_sql(dest_table, con=engine, if_exists="append", index=False)
        log.info("%-35s  inserted %d rows", dest_table, len(df))
        return len(df)

    except Exception as exc:
        log.error("%-35s  FAILED — %s", dest_table, exc)
        raise


def execute_on_dest(sql: str, description: str = ""):
    """Run a DDL/DML statement directly on the destination DB."""
    try:
        with pyodbc.connect(DEST_CONN_STR) as conn:
            conn.execute(sql)
            conn.commit()
        log.info("%-35s  OK", description or sql[:60])
    except Exception as exc:
        log.error("%-35s  FAILED — %s", description, exc)
        raise


# ---------------------------------------------------------------------------
# ETL Blocks
# ---------------------------------------------------------------------------

def load_dim_dist_center(engine):
    """Block 1 — Dim_DistCenter (incremental, NOT EXISTS)"""
    q = """
    SELECT dc.Id, DistCenterName, d.DistributorName, d.City, p.PrincipalName
    FROM   [vizlink-live].[dbo].DistCenters dc
           INNER JOIN [vizlink-live].[dbo].Distributors d ON d.Id = dc.DistributorId
           INNER JOIN [vizlink-live].[dbo].Principals   p ON p.Id = dc.PrincipalId
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Dim_DistCenter tgt
               WHERE  tgt.DistCenterId = dc.Id
           )
      AND  d.DistributorName NOT LIKE '%Tech%'
      AND  d.DistributorName NOT LIKE '%fjjdhrefc%'
      AND  d.DistributorName NOT LIKE '%Vizpro Test Distributor%'
    """
    load_incremental(q, "Dim_DistCenter", engine)


def load_dim_users(engine):
    """Block 2 — Dim_Users (incremental, RoleId = 4 only)"""
    q = """
    SELECT u.Id            AS UserId,
           u.FirstName, u.LastName,
           u.UserName       AS Username,
           u.CNIC,
           u.CreatedDate    AS SignupDate,
           CONVERT(nvarchar, u.CreatedDate, 112) AS SignUpDateKey,
           u.LastLogin      AS LastLoginDate
    FROM   [vizlink-live].[dbo].AspNetUsers u
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Dim_Users tgt WHERE tgt.UserId = u.Id
           )
      AND  EXISTS (
               SELECT 1
               FROM   [vizlink-live].[dbo].AspNetUserRoles ur
               WHERE  ur.UserId = u.Id AND ur.RoleId = 4
           )
    """
    load_incremental(q, "Dim_Users", engine)


def load_dim_dms(engine):
    """Block 3 — Dim_DMs"""
    q = """
    SELECT u.Id AS DMId, u.FirstName, u.LastName, u.QRCode, dc.DistCenterName
    FROM   [vizlink-live].[dbo].AspNetUsers u
           INNER JOIN [vizlink-live].[dbo].DistCenters dc ON dc.Id = u.DistCenterId
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Dim_DMs tgt WHERE tgt.DMID = u.Id
           )
    """
    load_incremental(q, "Dim_DMs", engine)


def load_dim_vizshops(engine):
    """Block 4 — Dim_Vizshops"""
    q = """
    SELECT vs.Id            AS VizShopId,
           vs.VizShopCode, vs.VizshopName, vs.CNIC, vs.ShopCategory,
           0                AS SectorNumber,
           rso.FirstName + ' ' + rso.LastName AS RsoName,
           vs.CreatedDate   AS InductionDate,
           vs.ModifiedDate, vs.Locality, vs.Area,
           CASE WHEN t.VizShopCode IS NULL THEN 'No' ELSE 'Yes' END AS SignUp,
           vs.lat, vs.long
    FROM   [vizlink-live].[dbo].VizShops vs
           LEFT JOIN [vizlink-live].[dbo].AspNetUsers rso ON rso.Id = vs.RsoId
           LEFT JOIN (
               SELECT DISTINCT v.VizShopCode
               FROM   [vizlink-live].[dbo].VizShops v
                      INNER JOIN [vizlink-live].[dbo].AspNetUsers u ON v.CNIC = u.CNIC
           ) t ON vs.VizShopCode = t.VizShopCode
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Dim_Vizshops tgt WHERE tgt.VizShopId = vs.Id
           )
    """
    load_incremental(q, "Dim_Vizshops", engine)


def load_dim_distributor_shops(engine):
    """Block 5a — Dim_DistributorShop"""
    q = """
    SELECT ds.Id AS DistributorShopId,
           dc.DistCenterKey, ds.ShopCode, ds.ShopName,
           v.VizShopKey,
           u.FirstName + ' ' + u.LastName AS RsoName
    FROM   [vizlink-live].[dbo].DistributorShops ds
           LEFT JOIN [VDWH_Auto].[dbo].Dim_Vizshops   v  ON v.VizShopId    = ds.VizShopId
           LEFT JOIN [VDWH_Auto].[dbo].Dim_DistCenter dc ON dc.DistCenterId = ds.DistCenterId
           LEFT JOIN [VDWH_Auto].[dbo].Dim_Users      u  ON u.UserId        = ds.InductionRsoId
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Dim_DistributorShop tgt
               WHERE  tgt.DistributorShopId = ds.Id
           )
    """
    load_incremental(q, "Dim_DistributorShop", engine)


def update_dim_distributor_shops():
    """Block 5b — Fix NULL VizShopKey rows"""
    sql = """
    UPDATE dsw
    SET    dsw.VizShopKey = v.VizShopKey
    FROM   VDWH_Auto.dbo.Dim_DistributorShop dsw
           INNER JOIN [vizlink-live].dbo.DistributorShops ds ON ds.Id = dsw.DistributorShopId
           INNER JOIN VDWH_Auto.dbo.Dim_Vizshops          v  ON v.VizShopId = ds.VizShopId
    WHERE  dsw.VizShopKey IS NULL OR dsw.VizShopKey <> v.VizShopKey
    """
    execute_on_dest(sql, "Update Dim_DistributorShop.VizShopKey")


def load_fact_shop_orders(engine):
    """Block 6 — Fact_ShopOrders"""
    q = """
    SELECT o.Id AS ShopOrderId,
           ds.DistributorShopKey, dc.DistCenterKey, v.VizShopKey,
           o.DeliveryManId, o.DeliveryManName, o.OrderDate, o.OrderDeliveryDate,
           o.NetSales, o.OriginalNetSales, o.InvoiceNumber,
           o.VizproInvoiceNumber AS DistributorInvoiceNumber,
           o.OrderAmountToPay, o.AmountPaid, o.OrderStatus
    FROM   [vizlink-live].dbo.ShopOrders o
           LEFT JOIN [VDWH_Auto].[dbo].Dim_DistributorShop ds ON ds.DistributorShopId = o.ShopId
           LEFT JOIN [VDWH_Auto].[dbo].Dim_DistCenter      dc ON dc.DistCenterId      = o.DistCenterId
           LEFT JOIN [VDWH_Auto].[dbo].Dim_Vizshops        v  ON v.VizShopId          = o.VizshopId
    WHERE  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Fact_ShopOrders tgt WHERE tgt.ShopOrderId = o.Id
           )
    """
    load_incremental(q, "Fact_ShopOrders", engine)


def load_fact_shop_transactions(engine):
    """Block 7 — Fact_ShopOrderTransactions (Paid only)"""
    q = """
    SELECT DISTINCT
           tr.Id            AS ShopTransactionId,
           tr.TransactionId, tr.PaymentType,
           tr.Amount        AS AmountPaid,
           tr.OrderStatus   AS TransactionStatus,
           tr.CreatedDate   AS PaymentDate,
           tr.MfiType, tr.TillId, tr.SenderContactNo, tr.DeliveryQRCode,
           dm.FirstName + ' ' + dm.LastName AS DeliverymanName,
           'Paid'           AS ModifiedorActualStatus,
           0                AS JazzTRID,
           o.ShopOrderKey
    FROM   [vizlink-live].dbo.ShopOrderTransactions tr
           INNER JOIN [VDWH_Auto].[dbo].Fact_ShopOrders o  ON o.ShopOrderId = tr.ShopOrderId
           INNER JOIN [VDWH_Auto].[dbo].Dim_DMs         dm ON dm.QRCode     = tr.DeliveryQRCode
    WHERE  tr.OrderStatus = 'Paid'
      AND  NOT EXISTS (
               SELECT 1 FROM [VDWH_Auto].[dbo].Fact_ShopOrderTransactions tgt
               WHERE  tgt.ShopTransactionId = tr.Id
           )
    """
    load_incremental(q, "Fact_ShopOrderTransactions", engine)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_etl():
    log.info("=" * 60)
    log.info("ETL Pipeline START")
    engine = get_engine()

    steps = [
        load_dim_dist_center,
        load_dim_users,
        load_dim_dms,
        load_dim_vizshops,
        load_dim_distributor_shops,
        update_dim_distributor_shops,
        load_fact_shop_orders,
        load_fact_shop_transactions,
    ]

    for step in steps:
        try:
            step(engine) if step != update_dim_distributor_shops else step()
        except Exception as exc:
            log.error("Pipeline stopped at %s: %s", step.__name__, exc)
            break

    log.info("ETL Pipeline END")
    log.info("=" * 60)


if __name__ == "__main__":
    run_etl()
