import duckdb
from .metrics import measure_resources, get_directory_size_mb


def run_proposed_queries(gold_dir: str = "data/gold") -> dict:
    """
    Thực thi các truy vấn phân tích tổng hợp (OLAP) trên tầng Gold Data Mart
    bằng động cơ nhúng DuckDB In-Memory (Zero-copy Columnar Execution).
    """
    print("\n" + "=" * 60)
    print(">>> KHOI CHAY PHUONG AN DE XUAT (DuckDB on Parquet Data Marts)")
    print("=" * 60)

    with measure_resources("DuckDB OLAP Queries") as m_proposed:
        conn = duckdb.connect()

        # Query 1: KPI Doanh thu GMV & Tổng đơn toàn sàn
        q_sales = conn.execute(
            f"SELECT sum(total_gmv), sum(total_orders) "
            f"FROM read_parquet('{gold_dir}/mart_monthly_sales/*.parquet')"
        ).fetchone()

        # Query 2: KPI Tỷ lệ giao hàng đúng hạn toàn sàn
        q_logistics = conn.execute(
            f"SELECT sum(on_time_orders) * 100.0 / sum(total_delivered_orders) "
            f"FROM read_parquet('{gold_dir}/mart_regional_logistics/*.parquet')"
        ).fetchone()

        # Query 3: KPI Khách hàng mua lại (Repeat Buyers)
        q_retention = conn.execute(
            f"SELECT sum(unique_customers) "
            f"FROM read_parquet('{gold_dir}/mart_customer_retention/*.parquet') "
            f"WHERE customer_segment = 'Repeat Buyer'"
        ).fetchone()

    proposed_gmv = round(q_sales[0] or 0.0, 2)
    proposed_on_time_rate = round(q_logistics[0] or 0.0, 2)
    proposed_repeat_cust = q_retention[0] or 0
    proposed_storage_mb = get_directory_size_mb(gold_dir)

    print(f"* Thoi gian truy van:     {m_proposed.duration_seconds:.3f} giay")
    print(f"* Muc tieu thu RAM dinh:  {m_proposed.peak_memory_mb:.2f} MB")
    print(f"* Dung luong Gold Marts:  {proposed_storage_mb:.2f} MB")
    print(f"* GMV tinh duoc:          R$ {proposed_gmv:,}")
    print(f"* Ty le giao dung han:    {proposed_on_time_rate}%")
    print(f"* Khach hang mua lai:     {proposed_repeat_cust:,} nguoi")
    print("=" * 60 + "\n")

    return {
        "query_time_s": m_proposed.duration_seconds,
        "peak_ram_mb": m_proposed.peak_memory_mb,
        "storage_mb": proposed_storage_mb,
        "gmv": proposed_gmv,
        "on_time_rate": proposed_on_time_rate,
        "repeat_customers": proposed_repeat_cust,
    }
