import os
from pyspark.sql import DataFrame


# Ánh xạ từ khóa bảng sang tên tệp/thư mục CSV chuẩn của bộ dữ liệu Olist
TABLE_TO_FILENAME = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "translations": "product_category_name_translation.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}


def load(bronze_tables: dict[str, DataFrame], output_dir: str = "data/bronze"):
    """
    Ghi toàn bộ các DataFrame của tầng Bronze ra thư mục output_dir dưới định dạng CSV thô.
    Các tên tệp khớp chính xác với quy ước đọc của tầng Silver (silver/extract.py).
    
    :param bronze_tables: Dictionary chứa các DataFrame ở tầng Bronze.
    :param output_dir: Thư mục đích lưu trữ dữ liệu Bronze (mặc định 'data/bronze').
    """
    print(f">>> [Bronze Load] Đang nạp dữ liệu Bronze vào '{output_dir}'...")
    os.makedirs(output_dir, exist_ok=True)

    for key, df in bronze_tables.items():
        filename = TABLE_TO_FILENAME.get(key, f"{key}.csv")
        destination = os.path.join(output_dir, filename).replace("\\", "/")
        print(f"    - Writing {key} -> {destination}")
        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .option("multiLine", "true")
            .csv(destination)
        )

    print(f">>> [Bronze Load] Hoàn tất nạp dữ liệu tầng Bronze vào '{output_dir}'!")
