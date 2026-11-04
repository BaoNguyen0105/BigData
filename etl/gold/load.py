from pyspark.sql import DataFrame


def load(gold_tables: dict[str, DataFrame], output_dir: str = "data/gold"):
    """
    Ghi toàn bộ các bảng Data Mart tầng Gold ra hệ thống lưu trữ định dạng Apache Parquet (nén Snappy).
    Các tệp Parquet này sẵn sàng để DuckDB / Streamlit Dashboard truy vấn tức thì.
    
    :param gold_tables: Dictionary chứa các Data Mart DataFrame.
    :param output_dir: Thư mục đích tầng Gold (mặc định 'data/gold').
    """
    print(f">>> [Gold Load] Đang lưu trữ Data Marts vào '{output_dir}/' định dạng Parquet...")

    for table_name, df in gold_tables.items():
        destination = f"{output_dir}/{table_name}"
        print(f"    - Writing {table_name} -> {destination}")
        df.write.mode("overwrite").parquet(destination)

    print(f">>> [Gold Load] Hoàn tất nạp dữ liệu tầng Gold vào '{output_dir}'!")

