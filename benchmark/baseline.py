import os
import sys
import numpy as np
import pandas as pd
from .metrics import measure_resources, get_directory_size_mb

# Đảm bảo in tiếng Việt an toàn trên Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def read_table_csv(base_dir: str, filename: str, **kwargs) -> pd.DataFrame:
    """
    Đọc tệp hoặc thư mục phân vùng CSV vào Pandas DataFrame.
    Hỗ trợ cả tệp CSV đơn lẻ (dataset/) hoặc thư mục chứa part-*.csv của Spark (data/bronze/).
    """
    target_path = os.path.join(base_dir, filename)
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"Không tìm thấy dữ liệu tại: {target_path}")

    if os.path.isfile(target_path):
        return pd.read_csv(target_path, low_memory=False, **kwargs)

    part_files = [
        os.path.join(target_path, f)
        for f in os.listdir(target_path)
        if f.startswith("part-") and f.endswith(".csv")
    ]
    if not part_files:
        raise FileNotFoundError(f"Không tìm thấy tệp CSV part-* nào trong thư mục: {target_path}")

    dfs = [pd.read_csv(f, low_memory=False, **kwargs) for f in part_files]
    return pd.concat(dfs, ignore_index=True)


def calculate_baseline_monthly_sales(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    products: pd.DataFrame,
    translations: pd.DataFrame,
) -> pd.DataFrame:
    """Tính toán KPI 1 (Doanh số GMV & Ngành hàng) bằng Pandas truyền thống."""
    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="left")
    merged = merged.merge(translations, on="product_category_name", how="left")

    merged["product_category_name_english"] = merged["product_category_name_english"].fillna("unknown")

    purchase_dates = pd.to_datetime(merged["order_purchase_timestamp"], errors="coerce")
    merged["year"] = purchase_dates.dt.year.astype("Int64")
    merged["quarter"] = purchase_dates.dt.quarter.astype("Int64")
    merged["month"] = purchase_dates.dt.month.astype("Int64")
    merged["month_name"] = purchase_dates.dt.strftime("%B")
    merged["total_item_value"] = merged["price"] + merged["freight_value"]

    grouped = (
        merged
        .groupby(["year", "quarter", "month", "month_name", "product_category_name_english"], dropna=True)
        .agg(
            total_gmv=("total_item_value", "sum"),
            total_item_price=("price", "sum"),
            total_freight_value=("freight_value", "sum"),
            total_orders=("order_id", "nunique"),
            total_items_sold=("order_item_id", "count"),
        )
        .reset_index()
    )

    grouped["total_gmv"] = grouped["total_gmv"].round(2)
    grouped["total_item_price"] = grouped["total_item_price"].round(2)
    grouped["total_freight_value"] = grouped["total_freight_value"].round(2)
    grouped["average_order_value"] = (grouped["total_gmv"] / grouped["total_orders"]).round(2)

    return grouped


def calculate_baseline_regional_logistics(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    customers: pd.DataFrame,
    reviews: pd.DataFrame,
) -> pd.DataFrame:
    """Tính toán KPI 3 & 4 (Giao đúng hạn & CSAT theo bang) bằng Pandas truyền thống."""
    delivered_orders = orders[orders["order_status"] == "delivered"].copy()

    reviews_clean = reviews.drop_duplicates(subset=["order_id"]).copy()
    reviews_clean["review_score"] = pd.to_numeric(reviews_clean["review_score"], errors="coerce")

    merged = delivered_orders.merge(customers[["customer_id", "customer_state"]], on="customer_id", how="inner")
    merged = merged.merge(order_items[["order_id", "freight_value"]], on="order_id", how="inner")
    merged = merged.merge(reviews_clean[["order_id", "review_score"]], on="order_id", how="left")

    delivered_date = pd.to_datetime(merged["order_delivered_customer_date"], errors="coerce")
    estimated_date = pd.to_datetime(merged["order_estimated_delivery_date"], errors="coerce")
    purchase_date = pd.to_datetime(merged["order_purchase_timestamp"], errors="coerce")

    merged["year"] = purchase_date.dt.year.astype("Int64")
    merged["delivery_delay_days"] = (delivered_date - estimated_date).dt.days

    # Gom nhóm cấp độ đơn hàng trước (tránh đếm trùng khi 1 đơn có nhiều mặt hàng)
    order_level = (
        merged
        .groupby(["order_id", "year", "customer_state"], as_index=False)
        .agg(
            delivery_delay_days=("delivery_delay_days", "first"),
            freight_value=("freight_value", "mean"),
            review_score=("review_score", "first"),
        )
    )
    order_level["is_on_time"] = (order_level["delivery_delay_days"] <= 0).astype(int)
    order_level["is_delayed"] = (order_level["delivery_delay_days"] > 0).astype(int)

    result = (
        order_level
        .groupby(["year", "customer_state"], as_index=False)
        .agg(
            total_delivered_orders=("order_id", "count"),
            on_time_orders=("is_on_time", "sum"),
            delayed_orders=("is_delayed", "sum"),
            avg_delivery_delay_days=("delivery_delay_days", "mean"),
            avg_freight_value=("freight_value", "mean"),
            avg_review_score=("review_score", "mean"),
        )
    )

    result["on_time_delivery_rate"] = np.where(
        result["total_delivered_orders"] > 0,
        (result["on_time_orders"] / result["total_delivered_orders"] * 100.0).round(2),
        0.0,
    )
    result["avg_delivery_delay_days"] = result["avg_delivery_delay_days"].round(2)
    result["avg_freight_value"] = result["avg_freight_value"].round(2)
    result["avg_review_score"] = result["avg_review_score"].round(2)
    return result


def calculate_baseline_customer_retention(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """Tính toán KPI 2 (Tỷ lệ giữ chân khách hàng) bằng Pandas truyền thống."""
    merged = orders.merge(customers[["customer_id", "customer_unique_id", "customer_state"]], on="customer_id", how="inner")
    merged = merged.merge(order_items[["order_id", "price", "freight_value"]], on="order_id", how="inner")
    merged["item_spend"] = merged["price"] + merged["freight_value"]

    cust_summary = (
        merged
        .groupby(["customer_unique_id", "customer_state"])
        .agg(
            customer_order_count=("order_id", "nunique"),
            customer_total_spend=("item_spend", "sum"),
        )
        .reset_index()
    )

    cust_summary["customer_segment"] = np.where(
        cust_summary["customer_order_count"] == 1,
        "One-time Buyer",
        "Repeat Buyer",
    )
    cust_summary["order_count_bucket"] = np.where(
        cust_summary["customer_order_count"] >= 4,
        4,
        cust_summary["customer_order_count"],
    )

    result = (
        cust_summary
        .groupby(["customer_state", "customer_segment", "order_count_bucket"])
        .agg(
            unique_customers=("customer_unique_id", "count"),
            total_orders=("customer_order_count", "sum"),
            total_spend=("customer_total_spend", "sum"),
        )
        .reset_index()
    )

    result["total_spend"] = result["total_spend"].round(2)
    result["avg_spend_per_customer"] = (result["total_spend"] / result["unique_customers"]).round(2)
    return result


def run_baseline_pipeline(
    input_dir: str = "data/bronze",
    output_dir: str = "data/baseline",
) -> dict:
    """
    Thực thi toàn bộ phương án Baseline (Python Pandas xử lý trực tiếp trên CSV thô).
    Ghi nhận chi tiết thời gian và mức tiêu thụ tài nguyên phục vụ bảng so sánh đối đầu.
    """
    print("\n" + "=" * 60)
    print(">>> KHOI CHAY PHUONG AN BASELINE (Traditional Pandas on Raw CSV)")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)

    # 1. ĐO THỜI GIAN VÀ BỘ NHỚ KHI NẠP TẤT CẢ FILE CSV
    with measure_resources("1. Nap CSV tho vao Pandas") as m_load:
        print(f">>> [1/3] Dang nap cac tep CSV tho tu '{input_dir}' vao RAM...")
        orders = read_table_csv(input_dir, "olist_orders_dataset.csv")
        order_items = read_table_csv(input_dir, "olist_order_items_dataset.csv")
        customers = read_table_csv(input_dir, "olist_customers_dataset.csv")
        products = read_table_csv(input_dir, "olist_products_dataset.csv")
        translations = read_table_csv(input_dir, "product_category_name_translation.csv")
        reviews = read_table_csv(input_dir, "olist_order_reviews_dataset.csv", on_bad_lines="skip")

    total_raw_rows = len(orders) + len(order_items) + len(customers) + len(products) + len(reviews)
    print(f"    - Nap thanh cong {total_raw_rows:,} ban ghi tho.")
    print(f"    - Thoi gian nap: {m_load.duration_seconds:.3f}s | RAM dinh: {m_load.peak_memory_mb:.2f} MB")

    # 2. ĐO THỜI GIAN VÀ BỘ NHỚ KHI TÍNH TOÁN 3 KPI
    with measure_resources("2. Tinh toan KPI bang Pandas") as m_compute:
        print(">>> [2/3] Dang JOIN bang va tinh toan cac chi so KPI...")
        sales_df = calculate_baseline_monthly_sales(orders, order_items, products, translations)
        logistics_df = calculate_baseline_regional_logistics(orders, order_items, customers, reviews)
        retention_df = calculate_baseline_customer_retention(orders, order_items, customers)

    print(f"    - Thoi gian tinh toan: {m_compute.duration_seconds:.3f}s | RAM dinh: {m_compute.peak_memory_mb:.2f} MB")

    # 3. LƯU KẾT QUẢ VÀO data/baseline/
    print(f">>> [3/3] Dang luu tru ket qua phan tich vao '{output_dir}'...")
    sales_path = os.path.join(output_dir, "baseline_monthly_sales.csv")
    logistics_path = os.path.join(output_dir, "baseline_regional_logistics.csv")
    retention_path = os.path.join(output_dir, "baseline_customer_retention.csv")

    sales_df.to_csv(sales_path, index=False)
    logistics_df.to_csv(logistics_path, index=False)
    retention_df.to_csv(retention_path, index=False)

    baseline_size_mb = get_directory_size_mb(output_dir)
    total_time = round(m_load.duration_seconds + m_compute.duration_seconds, 3)
    peak_ram = max(m_load.peak_memory_mb, m_compute.peak_memory_mb)

    print("\n" + "=" * 60)
    print("TONG KET HIEU NANG PHUONG AN BASELINE (Pandas)")
    print("=" * 60)
    print(f"* So ban ghi dau vao:       {total_raw_rows:,} ban ghi")
    print(f"* Thoi gian nap CSV:        {m_load.duration_seconds:.3f} giay")
    print(f"* Thoi gian tinh toan KPI:  {m_compute.duration_seconds:.3f} giay")
    print(f"* Tong thoi gian xu ly:     {total_time:.3f} giay")
    print(f"* Muc tieu thu RAM dinh:    {peak_ram:.2f} MB")
    print(f"* Dung luong ket qua:       {baseline_size_mb:.2f} MB")
    print(f"* Tep luu tai:             '{output_dir}'")
    print("=" * 60 + "\n")

    return {
        "total_rows": total_raw_rows,
        "load_time_s": m_load.duration_seconds,
        "compute_time_s": m_compute.duration_seconds,
        "total_time_s": total_time,
        "peak_ram_mb": peak_ram,
        "storage_mb": baseline_size_mb,
    }
