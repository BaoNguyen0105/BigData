import sys
import argparse
from etl.shared import get_spark_session
from etl.bronze.extract import download_dataset, extract as bronze_extract
from etl.bronze.transform import multiply_data as bronze_transform
from etl.bronze.__main__ import run_bronze_pipeline

from etl.silver.transform import transform as silver_transform
from etl.silver.__main__ import run_silver_pipeline

from etl.gold.transform import transform as gold_transform
from etl.gold.load import load as gold_load
from etl.gold.__main__ import run_gold_pipeline

# Đảm bảo in tiếng Việt chuẩn trên Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def run_staged_pipeline(
    scale: int = 1,
    bronze_dir: str = "data/bronze",
    silver_dir: str = "data/silver",
    gold_dir: str = "data/gold",
    spark=None,
):
    """
    Quy trình Medallion phân đoạn tiêu chuẩn (Staged):
    Lưu từng tầng xuống đĩa (CSV thô -> Parquet Silver -> Parquet Gold)
    để phục vụ khả năng kiểm tra độc lập và truy vết (Data Lineage).
    """
    print("\n" + "=" * 60)
    print(f"🚀 BẮT ĐẦU QUY TRÌNH MEDALLION PHÂN ĐOẠN (Staged, Scale: {scale}x)")
    print("=" * 60 + "\n")

    owns_spark = False
    if spark is None:
        spark = get_spark_session(app_name=f"Staged-Medallion-{scale}x")
        owns_spark = True

    try:
        # Chặng 1: Bronze (Tải, nhân bản và lưu CSV thô)
        run_bronze_pipeline(times=scale, output_dir=bronze_dir, spark=spark)

        # Chặng 2: Silver (Đọc Bronze, làm sạch, lưu Parquet Star Schema)
        run_silver_pipeline(input_dir=bronze_dir, output_dir=silver_dir, spark=spark)

        # Chặng 3: Gold (Đọc Silver, tổng hợp, lưu Parquet Data Marts)
        run_gold_pipeline(input_dir=silver_dir, output_dir=gold_dir, spark=spark)

        print("\n" + "=" * 60)
        print("🎉 QUY TRÌNH MEDALLION PHÂN ĐOẠN ĐÃ HOÀN TẤT THÀNH CÔNG!")
        print(f"📁 Dữ liệu Gold sẵn sàng tại: '{gold_dir}'")
        print("=" * 60 + "\n")
    finally:
        if owns_spark:
            spark.stop()


def run_in_memory_pipeline(
    scale: int = 1,
    raw_dir: str = "dataset",
    gold_dir: str = "data/gold",
    no_load: bool = False,
    spark=None,
):
    """
    Quy trình biến đổi dữ liệu trực tiếp trong bộ nhớ (In-Memory Transformation Pipeline):
    Nối chuỗi Bronze Transform -> Silver Transform -> Gold Transform hoàn toàn trong RAM
    mà không cần ghi ra đĩa hoặc đọc lại trung gian giữa các tầng.
    """
    print("\n" + "=" * 60)
    print(f"⚡ PIPELINE BIẾN ĐỔI TRỰC TIẾP TRONG BỘ NHỚ (In-Memory, Scale: {scale}x)")
    print("=" * 60 + "\n")

    owns_spark = False
    if spark is None:
        spark = get_spark_session(app_name=f"InMemory-ETL-{scale}x")
        owns_spark = True

    try:
        # 1. Trích xuất dữ liệu gốc một lần duy nhất từ nguồn
        dataset_path = download_dataset(output_dir=raw_dir)
        raw_data = bronze_extract(spark, dataset_dir=dataset_path)

        # 2. Transform Bronze (nhân bản dữ liệu trong RAM) - không ghi đĩa
        print(">>> [In-Memory 1/3] Bronze Transform (Scale-up)...")
        scaled_data = bronze_transform(raw_data, times=scale)

        # 3. Transform Silver (xây dựng Star Schema trong RAM) - không đọc lại từ đĩa
        print(">>> [In-Memory 2/3] Silver Transform (Star Schema)...")
        silver_tables = silver_transform(scaled_data)

        # 4. Transform Gold (tổng hợp BI Data Marts trong RAM) - không đọc lại từ đĩa
        print(">>> [In-Memory 3/3] Gold Transform (Data Marts)...")
        gold_tables = gold_transform(silver_tables)

        # 5. Xử lý bước cuối cùng (Load hoặc Action Benchmark)
        if not no_load:
            print(f">>> [In-Memory Load] Ghi các Data Marts trực tiếp vào '{gold_dir}'...")
            gold_load(gold_tables, output_dir=gold_dir)
            print("\n" + "=" * 60)
            print("🎉 PIPELINE BIẾN ĐỔI TRONG BỘ NHỚ HOÀN TẤT THÀNH CÔNG!")
            print(f"📁 Dữ liệu Gold sẵn sàng tại: '{gold_dir}'")
            print("=" * 60 + "\n")
        else:
            print("\n>>> [--no-load] Bỏ qua bước ghi đĩa. Kích hoạt tính toán để đo lường...")
            for table_name, df in gold_tables.items():
                count = df.count()
                print(f"    - Mart '{table_name}': {count:,} bản ghi")
            print("\n" + "=" * 60)
            print("🎉 TÍNH TOÁN TRONG RAM HOÀN TẤT (Zero Disk Write)!")
            print("=" * 60 + "\n")

        return gold_tables
    finally:
        if owns_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Điều phối Pipeline Medallion (Bronze -> Silver -> Gold) cho học phần Big Data & BI (CO4033)."
    )
    parser.add_argument(
        "-s", "--scale",
        type=int,
        default=1,
        help="Hệ số nhân bản dữ liệu Big Data (mặc định: 1 = gốc, có thể chọn 10, 50 để scale-up)",
    )
    parser.add_argument(
        "--in-memory",
        action="store_true",
        help="Chạy toàn bộ transform liên hoàn trong bộ nhớ (bỏ qua extract & load trung gian giữa các tầng)",
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Chỉ thực thi transform trong RAM mà không ghi dữ liệu ra đĩa",
    )
    parser.add_argument(
        "--bronze-dir",
        type=str,
        default="data/bronze",
        help="Thư mục lưu trữ tầng Bronze",
    )
    parser.add_argument(
        "--silver-dir",
        type=str,
        default="data/silver",
        help="Thư mục lưu trữ tầng Silver",
    )
    parser.add_argument(
        "--gold-dir",
        type=str,
        default="data/gold",
        help="Thư mục lưu trữ tầng Gold",
    )
    args = parser.parse_args()

    if args.in_memory or args.no_load:
        run_in_memory_pipeline(
            scale=args.scale,
            gold_dir=args.gold_dir,
            no_load=args.no_load,
        )
    else:
        run_staged_pipeline(
            scale=args.scale,
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            gold_dir=args.gold_dir,
        )


if __name__ == "__main__":
    main()
