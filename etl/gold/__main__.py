import argparse
from etl.shared import get_spark_session
from .extract import extract
from .transform import transform
from .load import load


def run_gold_pipeline(
    input_dir: str = "data/silver",
    output_dir: str = "data/gold",
    spark=None,
) -> dict:
    """
    Thực thi toàn bộ quy trình tầng Gold (Presentation / Data Marts):
    1. Đọc dữ liệu Star Schema (Parquet) từ tầng Silver.
    2. Tổng hợp thành 3 bảng Data Mart phẳng theo các KPI nghiệp vụ.
    3. Ghi các Data Mart ra output_dir dưới dạng Parquet (nén Snappy).
    
    :return: Dictionary chứa các DataFrame Data Mart tầng Gold.
    """
    print("==========================================")
    print("🥇 KHỞI CHẠY PIPELINE TẦNG GOLD (BI Data Marts)")
    print("==========================================")

    owns_spark = False
    if spark is None:
        spark = get_spark_session(app_name="Gold-Pipeline-DataMarts")
        owns_spark = True

    try:
        # 1. Extract: Đọc từ tầng Silver
        silver_tables = extract(spark, silver_dir=input_dir)

        # 2. Transform: Tổng hợp ra 3 Data Marts
        gold_tables = transform(silver_tables)

        # 3. Load: Ghi Parquet vào data/gold/
        load(gold_tables, output_dir=output_dir)
        print("✅ Hoàn tất toàn bộ quy trình tầng Gold!\n")
        return gold_tables
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline tổng hợp Data Marts tầng Gold cho BI Dashboard.")
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        default="data/silver",
        help="Thư mục nguồn dữ liệu tầng Silver (mặc định: 'data/silver')",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="data/gold",
        help="Thư mục đích lưu trữ Parquet tầng Gold (mặc định: 'data/gold')",
    )
    args = parser.parse_args()

    run_gold_pipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
    )

