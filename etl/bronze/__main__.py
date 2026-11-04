import argparse
from etl.shared import get_spark_session
from .extract import download_dataset, extract
from .transform import multiply_data
from .load import load


def run_bronze_pipeline(
    times: int = 1,
    raw_dir: str = "dataset",
    output_dir: str = "data/bronze",
    spark=None,
):
    """
    Thực thi toàn bộ quy trình tầng Bronze:
    1. Kiểm tra và tải dữ liệu Olist gốc từ Kaggle về raw_dir (nếu chưa có).
    2. Đọc các tệp CSV thô thành PySpark DataFrames.
    3. Nhân bản dữ liệu (times)x để mô phỏng tải Big Data (1x, 10x, 50x).
    4. Ghi toàn bộ dữ liệu ra thư mục lưu trữ Bronze (output_dir).
    """
    print("==========================================")
    print(f"🚀 KHỞI CHẠY PIPELINE TẦNG BRONZE (Scale: {times}x)")
    print("==========================================")

    # 1. Tải dữ liệu từ Kaggle nếu chưa có sẵn
    dataset_path = download_dataset(output_dir=raw_dir)

    # 2. Khởi tạo SparkSession (tận dụng session truyền vào nếu có)
    owns_spark = False
    if spark is None:
        spark = get_spark_session(app_name=f"Bronze-Pipeline-{times}x")
        owns_spark = True

    try:
        # 3. Extract: Đọc CSV thô
        raw_data = extract(spark, dataset_dir=dataset_path)

        # 4. Transform: Nhân bản dữ liệu
        scaled_data = multiply_data(raw_data, times=times)

        # 5. Load: Ghi dữ liệu ra data/bronze/
        load(scaled_data, output_dir=output_dir)
        print("✅ Hoàn tất toàn bộ quy trình tầng Bronze!\n")
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline thu thập và mô phỏng dữ liệu tầng Bronze.")
    parser.add_argument(
        "-t", "--times",
        type=int,
        default=1,
        help="Hệ số nhân bản dữ liệu (mặc định: 1 = giữ nguyên, có thể chọn 10, 50 để scale-up)",
    )
    parser.add_argument(
        "-i", "--raw-dir",
        type=str,
        default="dataset",
        help="Thư mục chứa tệp CSV gốc tải từ Kaggle (mặc định: 'dataset')",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="data/bronze",
        help="Thư mục xuất dữ liệu tầng Bronze (mặc định: 'data/bronze')",
    )
    args = parser.parse_args()

    run_bronze_pipeline(
        times=args.times,
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
    )
