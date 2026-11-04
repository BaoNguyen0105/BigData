from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schema import (
    mart_monthly_sales_schema,
    mart_regional_logistics_schema,
    mart_customer_retention_schema,
)


def build_mart_monthly_sales(
    fact_order_items: DataFrame,
    dim_date: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """
    Xây dựng Data Mart Doanh số & Tăng trưởng theo Tháng / Quý / Năm và Ngành hàng.
    Phục vụ trực tiếp cho KPI 1 (GMV, AOV, Sales Trend, Top Categories).
    """
    joined = (
        fact_order_items
        .join(
            dim_date.select("date_key", "year", "quarter", "month", "month_name"),
            fact_order_items["purchase_date_key"] == dim_date["date_key"],
            how="inner",
        )
        .join(
            dim_product.select("product_key", "product_category_name_english"),
            on="product_key",
            how="left",
        )
    )

    agg_df = (
        joined
        .groupBy(
            "year",
            "quarter",
            "month",
            "month_name",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")).alias("product_category_name_english"),
        )
        .agg(
            F.round(F.sum("total_item_value"), 2).alias("total_gmv"),
            F.round(F.sum("price"), 2).alias("total_item_price"),
            F.round(F.sum("freight_value"), 2).alias("total_freight_value"),
            F.countDistinct("order_id").cast("int").alias("total_orders"),
            F.count("order_item_key").cast("int").alias("total_items_sold"),
        )
        .withColumn(
            "average_order_value",
            F.round(F.col("total_gmv") / F.col("total_orders"), 2),
        )
    )

    return agg_df.select([field.name for field in mart_monthly_sales_schema.fields])


def build_mart_regional_logistics(
    fact_order_items: DataFrame,
    dim_date: DataFrame,
    dim_customer: DataFrame,
) -> DataFrame:
    """
    Xây dựng Data Mart Hiệu suất Giao vận & Trải nghiệm khách hàng theo Bang.
    Phục vụ trực tiếp cho KPI 3 (On-Time Delivery Rate) và KPI 4 (CSAT & Lead Time).
    """
    # Chỉ tính toán trên các đơn hàng đã hoàn tất giao hàng
    delivered_facts = (
        fact_order_items
        .filter(F.col("order_status") == "delivered")
        .join(
            dim_date.select("date_key", "year"),
            fact_order_items["purchase_date_key"] == dim_date["date_key"],
            how="inner",
        )
        .join(
            dim_customer.select("customer_key", "customer_state", "latitude", "longitude"),
            on="customer_key",
            how="inner",
        )
    )

    agg_df = (
        delivered_facts
        .groupBy("year", "customer_state")
        .agg(
            F.countDistinct("order_id").cast("int").alias("total_delivered_orders"),
            F.countDistinct(
                F.when(F.col("delivery_delay_days") <= 0, F.col("order_id"))
            ).cast("int").alias("on_time_orders"),
            F.countDistinct(
                F.when(F.col("delivery_delay_days") > 0, F.col("order_id"))
            ).cast("int").alias("delayed_orders"),
            F.round(F.avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
            F.round(F.avg("freight_value"), 2).alias("avg_freight_value"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(F.avg("latitude"), 5).alias("latitude"),
            F.round(F.avg("longitude"), 5).alias("longitude"),
        )
        .withColumn(
            "on_time_delivery_rate",
            F.round(
                F.when(
                    F.col("total_delivered_orders") > 0,
                    (F.col("on_time_orders") / F.col("total_delivered_orders")) * 100.0,
                ).otherwise(0.0),
                2,
            ),
        )
    )

    return agg_df.select([field.name for field in mart_regional_logistics_schema.fields])


def build_mart_customer_retention(
    fact_order_items: DataFrame,
    dim_customer: DataFrame,
) -> DataFrame:
    """
    Xây dựng Data Mart Phân tích Giữ chân khách hàng & Giá trị vòng đời.
    Phục vụ trực tiếp cho KPI 2 (Customer Retention Rate & Phân khúc lòng trung thành).
    """
    # 1. Tổng hợp tần suất và chi tiêu theo từng khách hàng duy nhất (customer_unique_id)
    customer_orders = (
        fact_order_items
        .join(
            dim_customer.select("customer_key", "customer_unique_id", "customer_state"),
            on="customer_key",
            how="inner",
        )
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            F.countDistinct("order_id").cast("int").alias("customer_order_count"),
            F.round(F.sum("total_item_value"), 2).alias("customer_total_spend"),
        )
    )

    # 2. Gán nhãn phân khúc (Segment) và bucket số lượng đơn hàng
    segmented_customers = (
        customer_orders
        .withColumn(
            "customer_segment",
            F.when(F.col("customer_order_count") == 1, F.lit("One-time Buyer")).otherwise(F.lit("Repeat Buyer")),
        )
        .withColumn(
            "order_count_bucket",
            F.when(F.col("customer_order_count") >= 4, F.lit(4)).otherwise(F.col("customer_order_count")),
        )
    )

    # 3. Tổng hợp thành Data Mart phẳng theo Bang và Phân khúc
    agg_df = (
        segmented_customers
        .groupBy("customer_state", "customer_segment", "order_count_bucket")
        .agg(
            F.count("customer_unique_id").cast("int").alias("unique_customers"),
            F.sum("customer_order_count").cast("int").alias("total_orders"),
            F.round(F.sum("customer_total_spend"), 2).alias("total_spend"),
            F.round(F.avg("customer_total_spend"), 2).alias("avg_spend_per_customer"),
        )
    )

    return agg_df.select([field.name for field in mart_customer_retention_schema.fields])


def transform(silver_tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """
    Điều phối biến đổi dữ liệu từ tầng Silver sang 3 Data Marts tầng Gold.
    
    :param silver_tables: Dictionary chứa các bảng Star Schema tầng Silver.
    :return: Dictionary chứa 3 Data Mart DataFrame (mart_monthly_sales,
             mart_regional_logistics, mart_customer_retention).
    """
    print(">>> [Gold Transform] Đang tổng hợp các Data Marts tầng Gold...")

    fact_order_items = silver_tables["fact_order_items"]
    dim_date = silver_tables["dim_date"]
    dim_product = silver_tables["dim_product"]
    dim_customer = silver_tables["dim_customer"]

    mart_monthly_sales = build_mart_monthly_sales(
        fact_order_items=fact_order_items,
        dim_date=dim_date,
        dim_product=dim_product,
    )

    mart_regional_logistics = build_mart_regional_logistics(
        fact_order_items=fact_order_items,
        dim_date=dim_date,
        dim_customer=dim_customer,
    )

    mart_customer_retention = build_mart_customer_retention(
        fact_order_items=fact_order_items,
        dim_customer=dim_customer,
    )

    return {
        "mart_monthly_sales": mart_monthly_sales,
        "mart_regional_logistics": mart_regional_logistics,
        "mart_customer_retention": mart_customer_retention,
    }

