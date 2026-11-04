from pyspark.sql import SparkSession, DataFrame


def extract(spark: SparkSession, silver_dir: str = "data/silver") -> dict[str, DataFrame]:
    """
    Trích xuất toàn bộ các bảng Star Schema định dạng Parquet từ tầng Silver.
    
    :param spark: SparkSession đang hoạt động.
    :param silver_dir: Thư mục chứa dữ liệu tầng Silver (mặc định: 'data/silver').
    :return: Dictionary chứa các DataFrame dim_* và fact_order_items.
    """
    print(f">>> [Gold Extract] Đang trích xuất dữ liệu Star Schema từ '{silver_dir}'...")

    return {
        "dim_customer": spark.read.parquet(f"{silver_dir}/dim_customer"),
        "dim_seller": spark.read.parquet(f"{silver_dir}/dim_seller"),
        "dim_product": spark.read.parquet(f"{silver_dir}/dim_product"),
        "dim_date": spark.read.parquet(f"{silver_dir}/dim_date"),
        "fact_order_items": spark.read.parquet(f"{silver_dir}/fact_order_items"),
    }

