import argparse
from etl.shared import get_spark_session
from .extract import extract
from .transform import transform
from .load import load


def run_silver_pipeline(
    input_dir: str = "data/bronze",
    output_dir: str = "data/silver",
    spark=None,
) -> dict:
    """
    Thực thi toàn bộ quy trình tầng Silver (Star Schema Data Warehouse):
    1. Đọc dữ liệu CSV thô từ tầng Bronze (input_dir).
    2. Làm sạch, khử trùng, gom cụm Geolocation, sinh Surrogate Keys và tính chỉ số Fact.
    3. Ghi 4 bảng Dimension và 1 bảng Fact ra output_dir dưới dạng Parquet.
    
    :return: Dictionary chứa các DataFrame của mô hình Star Schema.
    """
    print("==========================================")
    print("🥈 KHỞI CHẠY PIPELINE TẦNG SILVER (Star Schema DW)")
    print("==========================================")

    owns_spark = False
    if spark is None:
        spark = get_spark_session(app_name="Silver-Pipeline-StarSchema")
        owns_spark = True

    try:
        # 1. Extract: Đọc từ tầng Bronze
        raw_data = extract(spark, dataset_dir=input_dir)

        # 2. Transform: Xây dựng Star Schema (dim_*, fact_order_items)
        warehouse_tables = transform(raw_data)

        # 3. Load: Ghi Parquet vào data/silver/
        load(warehouse_tables, output_dir=output_dir)
        print("✅ Hoàn tất toàn bộ quy trình tầng Silver!\n")
        return warehouse_tables
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline làm sạch và xây dựng Star Schema tầng Silver.")
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        default="data/bronze",
        help="Thư mục nguồn dữ liệu tầng Bronze (mặc định: 'data/bronze')",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="data/silver",
        help="Thư mục đích lưu trữ Parquet tầng Silver (mặc định: 'data/silver')",
    )
    args = parser.parse_args()

    run_silver_pipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
    )

