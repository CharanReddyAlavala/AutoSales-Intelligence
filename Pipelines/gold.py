from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, count, avg, date_format, datediff

# =========================================================
# 🏆 EXECUTIVE SUMMARY (TOP KPIs)
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_executive_summary")
def gold_executive_summary():

    df = spark.read.table("autosales.silver.silver_enriched")

    return df.agg(
        sum("selling_price").alias("total_revenue"),
        count("sale_id").alias("total_sales"),
        count("lead_id").alias("total_leads"),
        avg("selling_price").alias("avg_price"),
        avg("inventory_age_days").alias("avg_inventory_age"),
        avg("lead_response_time").alias("avg_response_time")
    )

# =========================================================
# 🏙️ CITY PERFORMANCE
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_city_performance")
def gold_city_performance():

    df = spark.read.table("autosales.silver.silver_enriched")

    return (
        df.groupBy("showroom_city")
          .agg(
              sum("selling_price").alias("revenue"),
              count("sale_id").alias("sales"),
              avg("selling_price").alias("avg_price")
          )
    )

# =========================================================
# 🚗 VEHICLE PERFORMANCE
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_vehicle_performance")
def gold_vehicle_performance():

    df = spark.read.table("autosales.silver.silver_enriched")

    return (
        df.groupBy("vehicle_name")
          .agg(
              count("sale_id").alias("units_sold"),
              avg("selling_price").alias("avg_price"),
              avg("inventory_age_days").alias("avg_inventory_days")
          )
    )

# =========================================================
# 📈 SALES TREND (TIME SERIES)
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_sales_trend")
def gold_sales_trend():

    df = spark.read.table("autosales.silver.silver_enriched")

    return (
        df.withColumn("month", date_format(col("sale_date"), "yyyy-MM"))
          .groupBy("month")
          .agg(
              sum("selling_price").alias("revenue"),
              count("sale_id").alias("sales")
          )
    )

# =========================================================
# 📞 LEAD CONVERSION FUNNEL
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_conversion_funnel")
def gold_conversion_funnel():

    df = spark.read.table("autosales.silver.silver_enriched")

    return (
        df.groupBy("showroom_city")
          .agg(
              count("lead_id").alias("total_leads"),
              count("sale_id").alias("total_sales")
          )
    )

# =========================================================
# 🔧 SERVICE KPI
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_service_kpi")
def gold_service_kpi():

    df = spark.read.table("autosales.silver.silver_enriched")

    return (
        df.groupBy("vehicle_name")
          .agg(
              sum("service_cost").alias("service_revenue"),
              avg("service_cost").alias("avg_service_cost")
          )
    )

# =========================================================
# 📢 CAMPAIGN PERFORMANCE (CORRECT VERSION)
# =========================================================
@dp.materialized_view(name="autosales.gold.gold_campaign_performance")
def gold_campaign_performance():

    df = spark.read.table("autosales.silver.silver_campaigns")

    return (
        df.select(
            col("campaign_id"),
            col("channel"),
            col("start_date"),
            col("end_date"),

            # Campaign duration
            datediff(col("end_date"), col("start_date")).alias("campaign_duration_days")
        )
    )