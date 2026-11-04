from pyspark.sql import DataFrame


def load(warehouse_tables: dict[str, DataFrame], output_dir: str = "data/silver"):
    """
    Ghi toàn bộ các bảng của mô hình Star Schema (Facts & Dimensions)
    ra hệ thống lưu trữ phân vùng định dạng Apache Parquet (nén Snappy).
    
    :param warehouse_tables: Dictionary chứa các DataFrame dim_* và fact_order_items.
    :param output_dir: Thư mục đích tầng Silver (mặc định 'data/silver').
    """
    print(f">>> [Silver Load] Đang nạp Star Schema vào '{output_dir}/' định dạng Parquet...")

    for table_name, df in warehouse_tables.items():
        destination = f"{output_dir}/{table_name}"
        print(f"    - Writing {table_name} -> {destination}")
        df.write.mode("overwrite").parquet(destination)

    print(f">>> [Silver Load] Hoàn tất nạp dữ liệu tầng Silver vào '{output_dir}'!")
