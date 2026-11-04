from pyspark.sql import SparkSession, DataFrame


def extract(spark: SparkSession, dataset_dir: str = "data/bronze") -> dict[str, DataFrame]:
    """
    Đọc toàn bộ các tệp dữ liệu CSV từ tầng Bronze vào PySpark DataFrames.
    Tương thích với cả thư mục data/bronze/ hoặc dataset/ gốc.
    """
    print(f">>> [Silver Extract] Đang trích xuất dữ liệu từ '{dataset_dir}'...")

    data = {
        "customers": spark.read.csv(
            f"{dataset_dir}/olist_customers_dataset.csv", header=True, inferSchema=True
        ),
        "geolocation": spark.read.csv(
            f"{dataset_dir}/olist_geolocation_dataset.csv", header=True, inferSchema=True
        ),
        "order_items": spark.read.csv(
            f"{dataset_dir}/olist_order_items_dataset.csv", header=True, inferSchema=True
        ),
        "orders": spark.read.csv(
            f"{dataset_dir}/olist_orders_dataset.csv", header=True, inferSchema=True
        ),
        "products": spark.read.csv(
            f"{dataset_dir}/olist_products_dataset.csv", header=True, inferSchema=True
        ),
        "sellers": spark.read.csv(
            f"{dataset_dir}/olist_sellers_dataset.csv", header=True, inferSchema=True
        ),
        "translations": spark.read.csv(
            f"{dataset_dir}/product_category_name_translation.csv", header=True, inferSchema=True
        ),
        # multiLine=True là bắt buộc vì đánh giá của khách hàng chứa nhiều ký tự xuống dòng thô
        "reviews": (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .csv(f"{dataset_dir}/olist_order_reviews_dataset.csv")
        ),
    }
    return data
