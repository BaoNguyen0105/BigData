from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_key_suffix(raw_data: dict[str, DataFrame], suffix: str) -> dict[str, DataFrame]:
    """
    Gắn hậu tố (suffix) vào các trường khóa chính/khóa ngoại của các bảng giao dịch
    để tạo bản sao dữ liệu mới mà không bị xung đột khóa, đồng thời bảo toàn tính
    toàn vẹn tham chiếu (Referential Integrity) giữa các bảng.
    """
    # 1. Customers: gắn suffix vào customer_id và customer_unique_id
    customers_copy = (
        raw_data["customers"]
        .withColumn("customer_id", F.concat(F.col("customer_id"), F.lit(suffix)))
        .withColumn("customer_unique_id", F.concat(F.col("customer_unique_id"), F.lit(suffix)))
    )

    # 2. Orders: gắn suffix vào order_id và customer_id
    orders_copy = (
        raw_data["orders"]
        .withColumn("order_id", F.concat(F.col("order_id"), F.lit(suffix)))
        .withColumn("customer_id", F.concat(F.col("customer_id"), F.lit(suffix)))
    )

    # 3. Order Items: gắn suffix vào order_id
    order_items_copy = (
        raw_data["order_items"]
        .withColumn("order_id", F.concat(F.col("order_id"), F.lit(suffix)))
    )

    # 4. Reviews: gắn suffix vào review_id và order_id
    reviews_copy = (
        raw_data["reviews"]
        .withColumn("review_id", F.concat(F.col("review_id"), F.lit(suffix)))
        .withColumn("order_id", F.concat(F.col("order_id"), F.lit(suffix)))
    )

    copied = {
        "customers": customers_copy,
        "orders": orders_copy,
        "order_items": order_items_copy,
        "reviews": reviews_copy,
    }

    # 5. Payments (nếu có): gắn suffix vào order_id
    if "payments" in raw_data:
        copied["payments"] = (
            raw_data["payments"]
            .withColumn("order_id", F.concat(F.col("order_id"), F.lit(suffix)))
        )

    return copied


def multiply_data(raw_data: dict[str, DataFrame], times: int = 1) -> dict[str, DataFrame]:
    """
    Nhân bản dữ liệu giao dịch lên `times` lần (ví dụ: 10x, 50x) để mô phỏng tải Big Data.
    Các bảng danh mục dùng chung (products, sellers, geolocation, translations) được giữ nguyên.
    
    :param raw_data: Dictionary chứa các DataFrame thô trích xuất từ CSV.
    :param times: Hệ số nhân bản (1 = giữ nguyên, 10 = nhân 10 lần, 50 = nhân 50 lần).
    :return: Dictionary chứa các DataFrame sau khi nhân bản.
    """
    if times <= 1:
        print(">>> [Bronze Transform] Scale factor = 1x (giữ nguyên dữ liệu gốc).")
        return raw_data

    print(f">>> [Bronze Transform] Đang nhân bản dữ liệu giao dịch lên {times}x (scale-up simulation)...")

    # Bắt đầu với tập dữ liệu gốc (bản sao 0)
    scaled_customers = raw_data["customers"]
    scaled_orders = raw_data["orders"]
    scaled_order_items = raw_data["order_items"]
    scaled_reviews = raw_data["reviews"]
    scaled_payments = raw_data.get("payments")

    # Nhân bản thêm (times - 1) bản sao
    for i in range(1, times):
        suffix = f"_copy_{i}"
        copied = add_key_suffix(raw_data, suffix)

        scaled_customers = scaled_customers.unionByName(copied["customers"])
        scaled_orders = scaled_orders.unionByName(copied["orders"])
        scaled_order_items = scaled_order_items.unionByName(copied["order_items"])
        scaled_reviews = scaled_reviews.unionByName(copied["reviews"])
        if scaled_payments is not None and "payments" in copied:
            scaled_payments = scaled_payments.unionByName(copied["payments"])

    # Tạo dictionary kết quả (giữ nguyên các bảng catalog, cập nhật các bảng giao dịch đã scale)
    result = dict(raw_data)
    result["customers"] = scaled_customers
    result["orders"] = scaled_orders
    result["order_items"] = scaled_order_items
    result["reviews"] = scaled_reviews
    if scaled_payments is not None:
        result["payments"] = scaled_payments

    print(f">>> [Bronze Transform] Hoàn thành nhân bản {times}x thành công!")
    return result
