import os
from typing import Optional, Dict, Any, List
import duckdb
import pandas as pd


def get_connection() -> duckdb.DuckDBPyConnection:
    """Khởi tạo kết nối DuckDB in-memory độc lập và tối ưu truy vấn."""
    return duckdb.connect(database=":memory:")


def get_filter_options(gold_dir: str = "data/gold") -> Dict[str, List[Any]]:
    """Lấy danh sách các giá trị hợp lệ cho bộ lọc (Năm, Ngành hàng, Bang)."""
    con = get_connection()
    sales_path = os.path.join(gold_dir, "mart_monthly_sales", "*.parquet").replace("\\", "/")
    logistics_path = os.path.join(gold_dir, "mart_regional_logistics", "*.parquet").replace("\\", "/")
    retention_path = os.path.join(gold_dir, "mart_customer_retention", "*.parquet").replace("\\", "/")

    # 1. Danh sách năm
    years = [
        int(r[0]) for r in con.execute(
            f"SELECT DISTINCT year FROM read_parquet('{sales_path}') WHERE year IS NOT NULL ORDER BY year"
        ).fetchall()
    ]

    # 2. Danh sách ngành hàng
    categories = [
        str(r[0]) for r in con.execute(
            f"SELECT DISTINCT product_category_name_english FROM read_parquet('{sales_path}') "
            f"WHERE product_category_name_english IS NOT NULL ORDER BY product_category_name_english"
        ).fetchall()
    ]

    # 3. Danh sách 27 bang Brazil (kết hợp cả logistics và retention)
    states = [
        str(r[0]) for r in con.execute(
            f"SELECT DISTINCT customer_state FROM read_parquet('{logistics_path}') "
            f"WHERE customer_state IS NOT NULL ORDER BY customer_state"
        ).fetchall()
    ]

    return {
        "years": years,
        "categories": categories,
        "states": states,
    }


def query_sales_overview(
    selected_year: Optional[int] = None,
    selected_category: Optional[str] = None,
    gold_dir: str = "data/gold",
) -> Dict[str, Any]:
    """
    Truy vấn Data Mart Doanh số (mart_monthly_sales):
    Hỗ trợ bộ lọc Năm và Ngành hàng sản phẩm.
    """
    con = get_connection()
    sales_path = os.path.join(gold_dir, "mart_monthly_sales", "*.parquet").replace("\\", "/")

    where_clauses = ["1=1"]
    if selected_year is not None:
        where_clauses.append(f"year = {selected_year}")
    if selected_category and selected_category != "Tất cả":
        escaped_cat = selected_category.replace("'", "''")
        where_clauses.append(f"product_category_name_english = '{escaped_cat}'")

    where_sql = " AND ".join(where_clauses)

    # 1. Headline KPIs
    kpi_query = f"""
        SELECT
            ROUND(COALESCE(SUM(total_gmv), 0.0), 2) AS total_gmv,
            ROUND(COALESCE(SUM(total_item_price), 0.0), 2) AS total_item_price,
            ROUND(COALESCE(SUM(total_freight_value), 0.0), 2) AS total_freight,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            COALESCE(SUM(total_items_sold), 0) AS total_items,
            ROUND(COALESCE(SUM(total_gmv) / NULLIF(SUM(total_orders), 0), 0.0), 2) AS avg_order_value
        FROM read_parquet('{sales_path}')
        WHERE {where_sql}
    """
    kpi_df = con.execute(kpi_query).df()
    kpis = kpi_df.to_dict(orient="records")[0] if not kpi_df.empty else {
        "total_gmv": 0.0, "total_item_price": 0.0, "total_freight": 0.0,
        "total_orders": 0, "total_items": 0, "avg_order_value": 0.0,
    }

    # 2. Monthly Trend Timeline
    monthly_query = f"""
        SELECT
            year,
            month,
            month_name,
            printf('%04d-%02d', year, month) AS period,
            ROUND(COALESCE(SUM(total_gmv), 0.0), 2) AS gmv,
            ROUND(COALESCE(SUM(total_item_price), 0.0), 2) AS item_price,
            ROUND(COALESCE(SUM(total_freight_value), 0.0), 2) AS freight_value,
            COALESCE(SUM(total_orders), 0) AS orders,
            ROUND(COALESCE(SUM(total_gmv) / NULLIF(SUM(total_orders), 0), 0.0), 2) AS aov
        FROM read_parquet('{sales_path}')
        WHERE {where_sql}
        GROUP BY year, month, month_name
        ORDER BY year ASC, month ASC
    """
    monthly_df = con.execute(monthly_query).df()

    # 3. Top 10 Product Categories by GMV
    category_query = f"""
        SELECT
            product_category_name_english AS category,
            ROUND(COALESCE(SUM(total_gmv), 0.0), 2) AS gmv,
            COALESCE(SUM(total_orders), 0) AS orders,
            COALESCE(SUM(total_items_sold), 0) AS items_sold
        FROM read_parquet('{sales_path}')
        WHERE {where_sql}
        GROUP BY product_category_name_english
        ORDER BY gmv DESC
        LIMIT 10
    """
    top_categories_df = con.execute(category_query).df()

    return {
        "kpis": kpis,
        "monthly_trend": monthly_df,
        "top_categories": top_categories_df,
    }


def query_logistics_overview(
    selected_year: Optional[int] = None,
    selected_state: Optional[str] = None,
    gold_dir: str = "data/gold",
) -> Dict[str, Any]:
    """
    Truy vấn Data Mart Vận hành & CSAT (mart_regional_logistics):
    Hỗ trợ bộ lọc Năm và Bang khách hàng.
    """
    con = get_connection()
    logistics_path = os.path.join(gold_dir, "mart_regional_logistics", "*.parquet").replace("\\", "/")

    # Điều kiện lọc cho KPIs
    where_clauses = ["1=1"]
    if selected_year is not None:
        where_clauses.append(f"year = {selected_year}")
    if selected_state and selected_state != "Tất cả":
        where_clauses.append(f"customer_state = '{selected_state}'")

    where_sql = " AND ".join(where_clauses)

    # 1. Headline KPIs (đảm bảo COALESCE không trả về NaN)
    kpi_query = f"""
        SELECT
            COALESCE(SUM(total_delivered_orders), 0) AS total_delivered,
            COALESCE(SUM(on_time_orders), 0) AS on_time_orders,
            COALESCE(SUM(delayed_orders), 0) AS delayed_orders,
            ROUND(COALESCE(100.0 * SUM(on_time_orders) / NULLIF(SUM(total_delivered_orders), 0), 0.0), 2) AS on_time_rate,
            ROUND(COALESCE(AVG(avg_delivery_delay_days), 0.0), 2) AS avg_delay_days,
            ROUND(COALESCE(AVG(avg_freight_value), 0.0), 2) AS avg_freight,
            ROUND(COALESCE(AVG(avg_review_score), 0.0), 2) AS avg_csat
        FROM read_parquet('{logistics_path}')
        WHERE {where_sql}
    """
    kpi_df = con.execute(kpi_query).df()
    kpis = kpi_df.to_dict(orient="records")[0] if not kpi_df.empty else {
        "total_delivered": 0, "on_time_orders": 0, "delayed_orders": 0,
        "on_time_rate": 0.0, "avg_delay_days": 0.0, "avg_freight": 0.0, "avg_csat": 0.0,
    }

    # 2. Regional State Breakdown (So sánh các bang theo Năm được chọn)
    year_clause = f"WHERE year = {selected_year}" if selected_year is not None else ""
    state_query = f"""
        SELECT
            customer_state,
            COALESCE(SUM(total_delivered_orders), 0) AS total_orders,
            COALESCE(SUM(on_time_orders), 0) AS on_time_orders,
            COALESCE(SUM(delayed_orders), 0) AS delayed_orders,
            ROUND(COALESCE(100.0 * SUM(on_time_orders) / NULLIF(SUM(total_delivered_orders), 0), 0.0), 2) AS on_time_rate,
            ROUND(COALESCE(AVG(avg_delivery_delay_days), 0.0), 2) AS avg_delay_days,
            ROUND(COALESCE(AVG(avg_freight_value), 0.0), 2) AS avg_freight,
            ROUND(COALESCE(AVG(avg_review_score), 0.0), 2) AS avg_csat,
            AVG(latitude) AS latitude,
            AVG(longitude) AS longitude
        FROM read_parquet('{logistics_path}')
        {year_clause}
        GROUP BY customer_state
        ORDER BY total_orders DESC
    """
    state_df = con.execute(state_query).df()

    return {
        "kpis": kpis,
        "state_breakdown": state_df,
    }


def query_retention_overview(
    selected_state: Optional[str] = None,
    gold_dir: str = "data/gold",
) -> Dict[str, Any]:
    """
    Truy vấn Data Mart Khách hàng & Tỷ lệ quay lại (mart_customer_retention):
    Hỗ trợ bộ lọc Bang khách hàng.
    """
    con = get_connection()
    retention_path = os.path.join(gold_dir, "mart_customer_retention", "*.parquet").replace("\\", "/")

    where_clauses = ["1=1"]
    if selected_state and selected_state != "Tất cả":
        where_clauses.append(f"customer_state = '{selected_state}'")

    where_sql = " AND ".join(where_clauses)

    # 1. Headline KPIs (đảm bảo COALESCE không trả về NaN)
    kpi_query = f"""
        SELECT
            COALESCE(SUM(unique_customers), 0) AS total_customers,
            COALESCE(SUM(CASE WHEN customer_segment = 'Repeat Buyer' THEN unique_customers ELSE 0 END), 0) AS repeat_customers,
            ROUND(
                COALESCE(100.0 * SUM(CASE WHEN customer_segment = 'Repeat Buyer' THEN unique_customers ELSE 0 END)
                / NULLIF(SUM(unique_customers), 0), 0.0), 2
            ) AS repeat_rate,
            ROUND(COALESCE(SUM(total_spend), 0.0), 2) AS total_spend,
            ROUND(
                COALESCE(100.0 * SUM(CASE WHEN customer_segment = 'Repeat Buyer' THEN total_spend ELSE 0 END)
                / NULLIF(SUM(total_spend), 0), 0.0), 2
            ) AS repeat_spend_share
        FROM read_parquet('{retention_path}')
        WHERE {where_sql}
    """
    kpi_df = con.execute(kpi_query).df()
    kpis = kpi_df.to_dict(orient="records")[0] if not kpi_df.empty else {
        "total_customers": 0, "repeat_customers": 0, "repeat_rate": 0.0,
        "total_spend": 0.0, "repeat_spend_share": 0.0,
    }

    # 2. Segment Comparison (One-time vs Repeat)
    segment_query = f"""
        SELECT
            customer_segment,
            COALESCE(SUM(unique_customers), 0) AS total_customers,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            ROUND(COALESCE(SUM(total_spend), 0.0), 2) AS total_spend,
            ROUND(COALESCE(SUM(total_spend) / NULLIF(SUM(unique_customers), 0), 0.0), 2) AS avg_spend_per_customer
        FROM read_parquet('{retention_path}')
        WHERE {where_sql}
        GROUP BY customer_segment
        ORDER BY total_spend DESC
    """
    segment_df = con.execute(segment_query).df()

    # 3. Frequency Bucket Distribution (1, 2, 3, 4+ đơn hàng)
    bucket_query = f"""
        SELECT
            printf('%d đơn hàng', order_count_bucket) AS bucket_label,
            order_count_bucket,
            COALESCE(SUM(unique_customers), 0) AS customer_count,
            COALESCE(SUM(total_orders), 0) AS order_count,
            ROUND(COALESCE(SUM(total_spend), 0.0), 2) AS total_spend
        FROM read_parquet('{retention_path}')
        WHERE {where_sql}
        GROUP BY order_count_bucket
        ORDER BY order_count_bucket ASC
    """
    bucket_df = con.execute(bucket_query).df()

    return {
        "kpis": kpis,
        "segment_comparison": segment_df,
        "bucket_distribution": bucket_df,
    }
