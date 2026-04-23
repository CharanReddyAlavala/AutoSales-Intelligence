from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, upper, trim, to_date, current_date, datediff
)

# =========================================================
# 🚗 VEHICLES
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_vehicles")
def silver_vehicles():
    df = spark.read.table("autosales.bronze.bronze_vehicles")

    return (
        df.dropDuplicates()
        .filter(col("selling_price") > 0)
        .withColumn("name", trim(col("name")))
        .withColumn("fuel", upper(trim(col("fuel"))))
        .withColumn("transmission", upper(trim(col("transmission"))))
        .withColumn("selling_price", col("selling_price").cast("double"))
        .withColumn("km_driven", col("km_driven").cast("int"))
        .withColumn("year", col("year").cast("int"))
        .withColumn("listing_date", to_date(col("listing_date")))
    )

# =========================================================
# 👤 CUSTOMERS
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_customers")
def silver_customers():
    return spark.read.table("autosales.bronze.bronze_customers").dropDuplicates()

# =========================================================
# 💰 SALES
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_sales")
def silver_sales():
    df = spark.read.table("autosales.bronze.bronze_sales")

    return (
        df.dropDuplicates()
        .filter(col("selling_price") > 0)
        .withColumn("selling_price", col("selling_price").cast("double"))
        .withColumn("sale_date", to_date(col("sale_date")))
    )

# =========================================================
# 📞 LEADS
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_leads")
def silver_leads():
    df = spark.read.table("autosales.bronze.bronze_leads")

    return (
        df.dropDuplicates()
        .withColumn("lead_date", to_date(col("lead_date")))
    )

# =========================================================
# 🏢 SHOWROOMS
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_showrooms")
def silver_showrooms():
    return spark.read.table("autosales.bronze.bronze_showrooms").dropDuplicates()

# =========================================================
# 🚗 TEST DRIVES
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_test_drives")
def silver_test_drives():
    df = spark.read.table("autosales.bronze.bronze_test_drives")

    return (
        df.dropDuplicates()
        .withColumn("test_drive_date", to_date(col("test_drive_date")))
    )

# =========================================================
# 🔧 SERVICE HISTORY
# =========================================================
@dp.materialized_view(name="autosales.silver.silver_service_history")
def silver_service_history():
    df = spark.read.table("autosales.bronze.bronze_service_history")

    return (
        df.dropDuplicates()
        .withColumn("service_date", to_date(col("service_date")))
        .withColumn("cost", col("cost").cast("double"))
    )

# =========================================================
# 📢 CAMPAIGNS (SEPARATE TABLE FOR KPI)
# =========================================================
@dp.materialized_view(
    name="autosales.silver.silver_campaigns",
    comment="Cleaned campaigns data for marketing KPIs"
)
def silver_campaigns():

    df = spark.read.table("autosales.bronze.bronze_campaigns")

    return (
        df.dropDuplicates()

        # Clean text columns
        .withColumn("channel", upper(trim(col("channel"))))

        # Standardize types
        .withColumn("campaign_id", col("campaign_id").cast("int"))
        .withColumn("budget", col("budget").cast("double"))

        # Dates
        .withColumn("start_date", to_date(col("start_date")))
        .withColumn("end_date", to_date(col("end_date")))
    )

# =========================================================
# 🔥 FINAL ENRICHED TABLE (ALL JOINS, KPI READY)
# =========================================================
@dp.materialized_view(
    name="autosales.silver.silver_enriched",
    comment="Fully enriched dataset for KPI and dashboard"
)
def silver_enriched():

    s = spark.read.table("autosales.silver.silver_sales").alias("s")
    v = spark.read.table("autosales.silver.silver_vehicles").alias("v")
    c = spark.read.table("autosales.silver.silver_customers").alias("c")
    l = spark.read.table("autosales.silver.silver_leads").alias("l")
    sh = spark.read.table("autosales.silver.silver_showrooms").alias("sh")
    td = spark.read.table("autosales.silver.silver_test_drives").alias("td")
    sv = spark.read.table("autosales.silver.silver_service_history").alias("sv")

    df = (
        s.join(v, "vehicle_id")
         .join(c, "customer_id")
         .join(sh, "showroom_id")
         .join(l, "lead_id", "left")
         .join(td, ["vehicle_id", "lead_id"], "left")
         .join(sv, "vehicle_id", "left")
    )

    return df.select(

        # 🔑 KEYS
        col("s.sale_id"),
        col("s.vehicle_id"),
        col("s.customer_id"),
        col("s.showroom_id"),
        col("s.lead_id"),

        # 🚗 VEHICLE
        col("v.name").alias("vehicle_name"),
        col("v.year"),
        col("v.fuel"),
        col("v.transmission"),
        col("v.km_driven"),

        # 👤 CUSTOMER
        col("c.customer_name"),

        # 🏢 SHOWROOM
        col("sh.city").alias("showroom_city"),

        # 💰 SALES
        col("s.selling_price"),
        col("s.sale_date"),

        # 📞 LEAD
        col("l.lead_date"),

        # 🚗 TEST DRIVE
        col("td.test_drive_date"),

        # 🔧 SERVICE
        col("sv.service_date"),
        col("sv.cost").alias("service_cost"),

        # 📊 DERIVED KPIs
        datediff(current_date(), col("v.listing_date")).alias("inventory_age_days"),
        datediff(col("s.sale_date"), col("l.lead_date")).alias("lead_response_time")
    )