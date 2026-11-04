from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# ==============================================================================
# DATA MART GOLD SCHEMA DEFINITIONS
# ==============================================================================
# Các bảng tổng hợp phẳng (Denormalized Flat Tables) phục vụ trực tiếp cho
# BI Dashboard (Streamlit / DuckDB) và các chỉ số KPI theo Rubric BTL CO4033.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Monthly Sales Data Mart Schema (mart_monthly_sales)
# Phục vụ: KPI 1 (Tổng doanh thu GMV, AOV, Tăng trưởng & Cơ cấu ngành hàng)
# ------------------------------------------------------------------------------
mart_monthly_sales_schema = StructType([
    StructField("year", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("month_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("total_gmv", DoubleType(), False),                # price + freight_value
    StructField("total_item_price", DoubleType(), False),         # Doanh thu thuần sản phẩm
    StructField("total_freight_value", DoubleType(), False),      # Tổng cước phí vận chuyển
    StructField("total_orders", IntegerType(), False),            # Số đơn hàng duy nhất
    StructField("total_items_sold", IntegerType(), False),        # Tổng số lượng món hàng bán ra
    StructField("average_order_value", DoubleType(), False),      # AOV = total_gmv / total_orders
])

# ------------------------------------------------------------------------------
# 2. Regional Logistics & CSAT Data Mart Schema (mart_regional_logistics)
# Phục vụ: KPI 3 (Tỷ lệ giao đúng hạn) & KPI 4 (Điểm hài lòng CSAT theo bang)
# ------------------------------------------------------------------------------
mart_regional_logistics_schema = StructType([
    StructField("year", IntegerType(), False),
    StructField("customer_state", StringType(), False),
    StructField("total_delivered_orders", IntegerType(), False),  # Tổng đơn đã giao
    StructField("on_time_orders", IntegerType(), False),          # Số đơn có delivery_delay_days <= 0
    StructField("delayed_orders", IntegerType(), False),          # Số đơn có delivery_delay_days > 0
    StructField("on_time_delivery_rate", DoubleType(), False),    # (on_time_orders / total) * 100%
    StructField("avg_delivery_delay_days", DoubleType(), True),   # Số ngày trễ trung bình
    StructField("avg_freight_value", DoubleType(), False),        # Cước phí trung bình / đơn
    StructField("avg_review_score", DoubleType(), True),          # CSAT trung bình (1 -> 5 sao)
    StructField("latitude", DoubleType(), True),                  # Tọa độ đại diện để vẽ bản đồ
    StructField("longitude", DoubleType(), True),                 # Tọa độ đại diện để vẽ bản đồ
])

# ------------------------------------------------------------------------------
# 3. Customer Retention & Lifetime Value Data Mart Schema (mart_customer_retention)
# Phục vụ: KPI 2 (Tỷ lệ giữ chân khách hàng Retention Rate & Phân khúc trung thành)
# ------------------------------------------------------------------------------
mart_customer_retention_schema = StructType([
    StructField("customer_state", StringType(), False),
    StructField("customer_segment", StringType(), False),         # 'One-time Buyer' vs 'Repeat Buyer'
    StructField("order_count_bucket", IntegerType(), False),      # Tần suất mua hàng: 1, 2, 3, 4+ đơn
    StructField("unique_customers", IntegerType(), False),        # Số lượng khách hàng duy nhất
    StructField("total_orders", IntegerType(), False),            # Tổng số đơn đặt
    StructField("total_spend", DoubleType(), False),              # Tổng giá trị chi tiêu (GMV đóng góp)
    StructField("avg_spend_per_customer", DoubleType(), False),   # Chi tiêu trung bình / khách hàng
])
