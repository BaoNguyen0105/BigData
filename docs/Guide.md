# LỘ TRÌNH TRIỂN KHAI BÀI TẬP LỚN MÔN CO4033
## PHÂN TÍCH DỮ LIỆU LỚN VÀ TRÍ TUỆ KINH DOANH (BIG DATA & BI)

* **Đề tài lựa chọn:** Đề tài 17 – Xây dựng hệ BI tổng hợp cho doanh nghiệp giả lập
* **Nền tảng dữ liệu:** Olist Brazilian E-Commerce Dataset (Kaggle)
* **Kế thừa kỹ thuật:** Tận dụng kiến trúc Star Schema, Pipeline PySpark và DuckDB OLAP từ môn DW-DSS (CO4031)

---

## TỔNG QUAN TIẾN ĐỘ & PHÂN BỔ RUBRIC (THANG ĐIỂM 100)

| Tiêu chí Rubric | Điểm | Hạng mục thực hiện chính | Tình trạng kế thừa & Cần bổ sung |
| :--- | :---: | :--- | :--- |
| **Bài toán và KPI** | **15** | Định nghĩa bối cảnh, 2 câu hỏi kiểm chứng, 3 KPI cốt lõi | Đã có định hướng; cần chuẩn hóa công thức toán học |
| **Dữ liệu & Quản lý dữ liệu** | **15** | Nguồn dữ liệu, schema, data quality, kịch bản scale-up | Đã có nguồn Olist; cần script nhân bản tăng tải (Big Data) |
| **Pipeline và tính đúng** | **20** | ETL PySpark, làm sạch, Star Schema, Data Mart Parquet | Đã có code ETL lõi; cần bổ sung bảng Data Mart tổng hợp |
| **Phân tích và so sánh** | **20** | Baseline (Pandas/CSV) vs. Đề xuất (Spark/DuckDB/Parquet) | **Cần làm mới hoàn toàn** (đo thời gian, dung lượng, RAM) |
| **BI và giá trị quyết định** | **15** | Dashboard tương tác (Streamlit), 3 nhận xét & hành động | **Cần làm mới** (xây dựng giao diện Web Dashboard) |
| **Báo cáo, demo, tái lập** | **15** | Báo cáo PDF, slide thuyết trình, repo Git + README.md | Soạn thảo theo mẫu quy định của học phần |

---

## KẾ HOẠCH HÀNH ĐỘNG CHI TIẾT THEO TỪNG BƯỚC

### BƯỚC 1: XÁC ĐỊNH BÀI TOÁN KINH DOANH VÀ HỆ THỐNG KPI (15 ĐIỂM)

1. **Bối cảnh & Đối tượng sử dụng:**
   * **Bối cảnh:** Sàn thương mại điện tử Olist tại thị trường Brazil.
   * **Người dùng cuối (End-users):** Ban Giám đốc điều hành (Executive Board/C-Level) và Trưởng bộ phận Vận hành/Kinh doanh (Operations/Sales Leads).
2. **2 Câu hỏi phân tích kinh doanh có thể kiểm chứng (Business Questions):**
   * *Câu hỏi 1 (Tăng trưởng & Giữ chân):* Cơ cấu doanh số và sự biến động tăng trưởng theo thời gian/danh mục hàng hóa như thế nào, và tỷ lệ khách hàng quay lại mua sắm (Retention Rate) giữa các khu vực địa lý ra sao?
   * *Câu hỏi 2 (Vận hành & Hài lòng khách hàng):* Mức độ trễ hạn giao hàng và cước phí vận chuyển tác động như thế nào đến điểm hài lòng của khách hàng (Review Score) trên từng bang?
3. **Định nghĩa 3 KPI cốt lõi bắt buộc (kèm công thức toán học):**
   * **KPI 1 – Tổng doanh thu (Gross Merchandise Value - GMV):**
     $$\text{GMV} = \sum (\text{price} + \text{freight\_value})$$
   * **KPI 2 – Tỷ lệ giữ chân khách hàng (Customer Retention Rate):**
     $$\text{Retention Rate} = \frac{\text{Số khách hàng có } \ge 2 \text{ đơn hàng (theo } \texttt{customer\_unique\_id}\text{)}}{\text{Tổng số khách hàng duy nhất}} \times 100\%$$
   * **KPI 3 – Tỷ lệ giao hàng đúng hạn (On-Time Delivery Rate):**
     $$\text{On-Time Rate} = \frac{\text{Số đơn có } \texttt{delivery\_delay\_days} \le 0}{\text{Tổng số đơn hàng đã giao thành công}} \times 100\%$$
   * *(Tùy chọn) KPI 4 – Điểm hài lòng khách hàng trung bình (Customer Satisfaction - CSAT):*
     $$\text{CSAT} = \frac{1}{N} \sum_{i=1}^{N} \text{review\_score}_i$$

---

### BƯỚC 2: KẾ THỪA PIPELINE ETL & ĐÓNG GÓI DATA MART PARQUET (15 ĐIỂM + 20 ĐIỂM)

1. **Tái sử dụng các mô-đun ETL PySpark sẵn có:**
   * Logic đọc an toàn ký tự ngắt dòng trong trường đánh giá: `multiLine=True` trên `olist_order_reviews_dataset.csv`.
   * Giải thuật gom cụm tính tọa độ trung bình $(\overline{lat}, \overline{lng})$ theo `zip_code_prefix` từ bảng `olist_geolocation_dataset.csv` để triệt tiêu hiện tượng Cartesian Product explosion.
   * Mô hình Star Schema với Surrogate Keys:
     * Fact: `fact_order_items`
     * Dimensions: `dim_customer`, `dim_seller`, `dim_product`, `dim_date` (Role-playing dimension).
2. **Bổ sung các bảng Data Mart tổng hợp (Gold Layer Aggregations):**
   * Xuất các bảng tổng hợp sẵn định dạng Parquet (nén Snappy) để tăng tốc độ phản hồi cho BI Dashboard:
     * `mart_monthly_sales.parquet`: Doanh thu, số lượng đơn theo tháng, quý, năm và ngành hàng.
     * `mart_regional_logistics.parquet`: Tỷ lệ giao trễ, cước phí trung bình, điểm đánh giá theo từng bang.
     * `mart_customer_retention.parquet`: Tần suất mua hàng và giá trị vòng đời khách hàng.

---

### BƯỚC 3: MÔ PHỎNG TĂNG TẢI & THỰC NGHIỆM ĐO ĐỐI CHIẾU BASELINE (20 ĐIỂM)
*(Hạng mục bắt buộc để lấy trọn điểm thực nghiệm phân tích theo Rubric)*

1. **Mô phỏng quy mô Big Data (Scale-up Script):**
   * Xây dựng script PySpark nhân bản dữ liệu theo phân phối gốc để tạo ra 2 kịch bản kiểm thử:
     * **Mức 1 (1x - Dữ liệu thực tế):** ~100.000 bản ghi (~120 MB CSV thô).
     * **Mức 2 (10x - 50x - Dữ liệu tăng tải):** Từ 1 triệu đến 5 triệu bản ghi (~1.2 GB - 6 GB) nhằm chứng minh năng lực xử lý phân tán và độ bền của pipeline.
2. **Thiết lập 2 cấu hình xử lý đối đầu:**
   * **Phương án Baseline (Truyền thống):** Dùng **Python Pandas / SQLite** đọc trực tiếp các tệp CSV thô, thực hiện JOIN 4-5 bảng và tính toán các chỉ số KPI.
   * **Phương án Đề xuất (Proposed Data Mart):** Dùng **PySpark ETL** chuyển đổi sang Data Mart lưu trữ **Parquet**, sau đó sử dụng **DuckDB In-Memory / Spark SQL** để truy vấn tính toán.
3. **Hoàn thiện bảng đo lường thực nghiệm (theo mẫu Mục V):**

| Tiêu chí đo lường | Baseline (CSV + Pandas/SQLite) | Giải pháp đề xuất (Parquet Data Mart + DuckDB) | Nhận xét & Giải thích kỹ thuật |
| :--- | :--- | :--- | :--- |
| **Số bản ghi đầu vào** | 100k $\rightarrow$ 5M dòng | 100k $\rightarrow$ 5M dòng | Giữ nguyên tập kiểm thử trên cùng phần cứng |
| **Thời gian nạp dữ liệu (s)** | Đo thực tế | Đo thực tế | PySpark tối ưu I/O phân vùng |
| **Thời gian truy vấn OLAP (s)** | Đo thực tế (Full Table Scan) | Đo thực tế (In-memory Vectorized) | Parquet chỉ quét cột cần thiết, bỏ qua I/O thừa |
| **Dung lượng lưu trữ (MB)** | ~120 MB (1x) $\rightarrow$ ~6 GB (50x) | ~18 MB (1x) $\rightarrow$ ~900 MB (50x) | Nén từ điển + Snappy giảm > 75% kích thước |
| **Mức tiêu thụ RAM đỉnh (MB)** | Dễ chạm ngưỡng OOM ở mức 50x | Kiểm soát tốt theo từng khối dữ liệu | DuckDB và Spark kiểm soát bộ nhớ hiệu quả |

---

### BƯỚC 4: XÂY DỰNG GIAO DIỆN BI DASHBOARD TRỰC QUAN (15 ĐIỂM)

Sử dụng framework **Streamlit** (hoặc Power BI) kết nối trực tiếp vào các tệp Parquet / DuckDB:

1. **Trang 1: Bảng điều hành tổng quan Doanh số (Sales Executive Overview):**
   * Khối thẻ chỉ số (KPI Metric Cards): Tổng GMV, Tổng đơn hàng, Doanh thu trung bình/đơn.
   * Biểu đồ đường (Line Chart): Xu hướng GMV qua các năm, quý, tháng (hỗ trợ Roll-up / Drill-down).
   * Biểu đồ cột (Bar Chart): Top 10 ngành hàng mang lại doanh thu cao nhất.
   * Bộ lọc tương tác (Filters): Chọn khoảng thời gian (Năm 2016-2018), chọn Bang, chọn Ngành hàng.
2. **Trang 2: Hiệu suất Vận hành & Chuỗi cung ứng (Logistics & Fulfillment):**
   * Khối chỉ số: Tỷ lệ giao hàng đúng hạn (On-Time Rate), Thời gian giao hàng trung bình (Lead Time).
   * Bản đồ nhiệt địa lý (Choropleth/Scatter Map): Hiển thị tỷ lệ trễ hạn và cước vận chuyển theo 27 bang của Brazil.
   * Biểu đồ phân tán (Scatter Plot): Mối tương quan giữa số ngày trễ hạn (`delivery_delay_days`) và điểm số hài lòng (`review_score`).
3. **Trang 3: Phân tích Khách hàng & Tỷ lệ Giữ chân (Customer Retention):**
   * Khối chỉ số: Tổng số khách hàng duy nhất, Tỷ lệ khách mua lại ($\ge 2$ lần).
   * Biểu đồ bánh/donut: Tỷ trọng đóng góp doanh thu giữa khách hàng mới vs. khách hàng quay lại.
   * Bảng thống kê tần suất đặt hàng (Cohort/Frequency Distribution).

---

### BƯỚC 5: TỔNG HỢP INSIGHT VÀ ĐỀ XUẤT QUYẾT ĐỊNH KINH DOANH (15 ĐIỂM)

Xây dựng tối thiểu 3 nhận xét chiến lược kèm phân tích chi phí và rủi ro:
1. **Nhận xét 1 (Bất cân xứng logistics theo vùng):**
   * *Hiện trạng:* Các bang khu vực Đông Bắc (như MA, BA, CE) có thời gian giao hàng trung bình trên 20 ngày và tỷ lệ trễ hạn cao gấp đôi so với bang São Paulo (SP), kéo theo điểm đánh giá trung bình tụt xuống dưới 3.8 sao.
   * *Hành động đề xuất:* Thiết lập các trung tâm trung chuyển (Fulfillment Hubs) vệ tinh hoặc liên kết với các nhà xe vận chuyển nội địa tại vùng Đông Bắc để rút ngắn thời gian chặng cuối.
2. **Nhận xét 2 (Thực trạng giữ chân khách hàng thấp):**
   * *Hiện trạng:* Hơn 95% khách hàng trên sàn Olist chỉ phát sinh 1 đơn hàng duy nhất trong suốt vòng đời.
   * *Hành động đề xuất:* Triển khai chương trình loyalty/voucher cho lần mua hàng thứ 2 trong vòng 30 ngày kể từ khi đơn hàng đầu tiên được giao thành công.
3. **Phân tích Chi phí, Rủi ro & Giới hạn:**
   * Đánh giá chi phí đầu tư hạ tầng vận chuyển so với mức độ giảm thiểu thất thoát đơn hủy và tăng trưởng doanh thu từ khách hàng trung thành.

---

### BƯỚC 6: ĐÓNG GÓI BÁO CÁO, KHO MÃ NGUỒN VÀ SLIDE DEMO (15 ĐIỂM)

1. **Báo cáo hoàn chỉnh (PDF/DOCX):**
   * Đầy đủ sơ đồ kiến trúc hệ thống (Data Architecture & Star Schema).
   * Bảng đo lường kết quả thực nghiệm chi tiết (chứng minh tính ưu việt của giải pháp).
   * Ảnh chụp màn hình các trang Dashboard và thảo luận sai số, rủi ro.
   * Bản kê khai liêm chính học thuật (Academic Integrity) và phân công trách nhiệm.
2. **Kho mã nguồn (GitHub Repo) & Tệp `README.md`:**
   * Viết tài liệu hướng dẫn thiết lập môi trường (Python virtualenv, cài thư viện qua `requirements.txt`).
   * Cung cấp tập lệnh chạy một bước (Single-command reproduction): Chạy ETL, chạy script kiểm thử benchmark, khởi chạy Dashboard.
3. **Slide thuyết trình & Kịch bản Demo:**
   * Slide tóm tắt (10-12 trang): Vấn đề $\rightarrow$ Kiến trúc $\rightarrow$ Kết quả Benchmark $\rightarrow$ Demo Dashboard $\rightarrow$ Khuyến nghị kinh doanh.
   * Video ngắn hoặc kịch bản trình diễn tương tác trực tiếp trong 5-7 phút.