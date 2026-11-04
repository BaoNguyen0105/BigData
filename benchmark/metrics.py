import os
import time
import math
import tracemalloc
from dataclasses import dataclass
from typing import Any, Callable, Optional
from contextlib import contextmanager


@dataclass
class BenchmarkMetric:
    """Chứa kết quả đo lường tài nguyên của một phép thử."""
    name: str
    duration_seconds: float
    peak_memory_mb: float
    output: Any = None

    def __str__(self) -> str:
        return (
            f"[{self.name}] Thời gian: {self.duration_seconds:.3f}s | "
            f"RAM đỉnh: {self.peak_memory_mb:.2f} MB"
        )


@contextmanager
def measure_resources(name: str = "Benchmark Task"):
    """
    Context manager đo lường thời gian thực thi (Wall-clock Time)
    và mức tiêu thụ RAM đỉnh (Peak Memory) của một đoạn mã.
    """
    tracemalloc.start()
    start_time = time.perf_counter()

    metric = BenchmarkMetric(name=name, duration_seconds=0.0, peak_memory_mb=0.0)

    try:
        yield metric
    finally:
        end_time = time.perf_counter()
        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        metric.duration_seconds = round(end_time - start_time, 4)
        metric.peak_memory_mb = round(peak_mem / (1024 * 1024), 2)


def benchmark_function(func: Callable, *args, name: Optional[str] = None, **kwargs) -> BenchmarkMetric:
    """
    Thực thi một hàm và tự động đo thời gian chạy cùng mức chiếm dụng RAM đỉnh.
    """
    task_name = name or func.__name__
    with measure_resources(task_name) as metric:
        metric.output = func(*args, **kwargs)
    return metric


def get_directory_size_mb(dir_path: str) -> float:
    """
    Tính toán tổng dung lượng thực tế của một thư mục (MB) trên đĩa cứng.
    """
    if not os.path.exists(dir_path):
        return 0.0

    total_bytes = 0
    if os.path.isfile(dir_path):
        total_bytes = os.path.getsize(dir_path)
    else:
        for root, _, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    total_bytes += os.path.getsize(file_path)

    return round(total_bytes / (1024 * 1024), 2)


def calculate_storage_efficiency(baseline_dir: str, proposed_dir: str) -> dict:
    """
    So sánh hiệu quả lưu trữ giữa giải pháp Baseline (CSV) và Đề xuất (Parquet).
    """
    baseline_mb = get_directory_size_mb(baseline_dir)
    proposed_mb = get_directory_size_mb(proposed_dir)

    reduction_pct = 0.0
    compression_ratio = 1.0

    if baseline_mb > 0:
        reduction_pct = round(((baseline_mb - proposed_mb) / baseline_mb) * 100.0, 2)
        if proposed_mb > 0:
            compression_ratio = round(baseline_mb / proposed_mb, 2)

    return {
        "baseline_size_mb": baseline_mb,
        "proposed_size_mb": proposed_mb,
        "reduction_percentage": reduction_pct,
        "compression_ratio": compression_ratio,
    }


def verify_metric_parity(
    baseline_val: float,
    proposed_val: float,
    metric_name: str = "Metric",
    tolerance_pct: float = 0.01,
) -> bool:
    """
    Kiểm chứng độ khớp (Parity / Correctness) của chỉ số tính ra giữa 2 phương án.
    """
    if math.isclose(baseline_val, proposed_val, rel_tol=tolerance_pct):
        print(f"✅ [Parity Check] '{metric_name}' khớp chính xác: Baseline={baseline_val:,} | Proposed={proposed_val:,}")
        return True
    else:
        diff = abs(baseline_val - proposed_val)
        print(f"⚠️ [Parity Check] '{metric_name}' có chênh lệch: Baseline={baseline_val:,} | Proposed={proposed_val:,} (Sai lệch: {diff:,})")
        return False

