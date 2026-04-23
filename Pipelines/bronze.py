from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

BASE_PATH = "s3a://autosales-df/"

# ---------- Common Loader ----------
def bronze_loader(file_name):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(BASE_PATH + file_name)
        .withColumn("ingest_time", current_timestamp())
    )

# ---------- Vehicles ----------
@dp.materialized_view(name="autosales.bronze.bronze_vehicles")
def bronze_vehicles():
    return bronze_loader("vehicles.csv")

# ---------- Customers ----------
@dp.materialized_view(name="autosales.bronze.bronze_customers")
def bronze_customers():
    return bronze_loader("customers.csv")

# ---------- Sales ----------
@dp.materialized_view(name="autosales.bronze.bronze_sales")
def bronze_sales():
    return bronze_loader("sales.csv")

# ---------- Leads ----------
@dp.materialized_view(name="autosales.bronze.bronze_leads")
def bronze_leads():
    return bronze_loader("leads.csv")

# ---------- Showrooms ----------
@dp.materialized_view(name="autosales.bronze.bronze_showrooms")
def bronze_showrooms():
    return bronze_loader("showrooms.csv")

# ---------- Test Drives ----------
@dp.materialized_view(name="autosales.bronze.bronze_test_drives")
def bronze_test_drives():
    return bronze_loader("test_drives.csv")

# ---------- Service History ----------
@dp.materialized_view(name="autosales.bronze.bronze_service_history")
def bronze_service_history():
    return bronze_loader("service_history.csv")

# ---------- Campaigns ----------
@dp.materialized_view(name="autosales.bronze.bronze_campaigns")
def bronze_campaigns():
    return bronze_loader("campaigns.csv")
