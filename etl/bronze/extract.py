import os
import kagglehub
from pyspark.sql import SparkSession, DataFrame


def download_dataset(output_dir="dataset"):
    """
    Kiểm tra xem thư mục output_dir đã có các file CSV hay chưa.
    Nếu đã có đủ dữ liệu thì bỏ qua, nếu chưa có thì mới tải từ Kaggle.
    """
    if output_dir and os.path.exists(output_dir):
        csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
        if len(csv_files) >= 8:  # Bộ dữ liệu Olist có 8-9 file CSV
            print(f">>> Dataset đã có sẵn tại '{output_dir}' ({len(csv_files)} files CSV). Bỏ qua tải lại!")
            return output_dir

    print(f">>> Đang tải dataset từ Kaggle về '{output_dir}'...")
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce", output_dir=output_dir)
    return path

# 2. Hàm đọc các file CSV thô vào PySpark DataFrames (cần bổ sung)
def extract(spark: SparkSession, dataset_dir: str = "dataset") -> dict[str, DataFrame]:
    """Đọc tất cả các tệp CSV thô thành Spark DataFrames."""
    print(f">>> [Bronze Extract] Đang nạp các tệp CSV thô từ '{dataset_dir}'...")
    
    return {
        "customers": spark.read.csv(f"{dataset_dir}/olist_customers_dataset.csv", header=True, inferSchema=True),
        "geolocation": spark.read.csv(f"{dataset_dir}/olist_geolocation_dataset.csv", header=True, inferSchema=True),
        "order_items": spark.read.csv(f"{dataset_dir}/olist_order_items_dataset.csv", header=True, inferSchema=True),
        "orders": spark.read.csv(f"{dataset_dir}/olist_orders_dataset.csv", header=True, inferSchema=True),
        "products": spark.read.csv(f"{dataset_dir}/olist_products_dataset.csv", header=True, inferSchema=True),
        "sellers": spark.read.csv(f"{dataset_dir}/olist_sellers_dataset.csv", header=True, inferSchema=True),
        "translations": spark.read.csv(f"{dataset_dir}/product_category_name_translation.csv", header=True, inferSchema=True),
        "reviews": (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .csv(f"{dataset_dir}/olist_order_reviews_dataset.csv")
        ),
    }