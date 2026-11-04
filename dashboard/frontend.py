import os
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Thêm thư mục gốc vào PYTHONPATH
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dashboard.query import (
    get_filter_options,
    query_sales_overview,
    query_logistics_overview,
    query_retention_overview,
)

# ------------------------------------------------------------------------------
# 1. CẤU HÌNH TRANG STREAMLIT
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="BI Dashboard - Doanh Nghiệp E-Commerce (Olist)",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Tuỳ biến CSS cho giao diện trực quan và chuyên nghiệp
st.markdown(
    """
    <style>
    /* Metric cards styling */
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 16px;
        border: 1px solid #e9ecef;
        box-shadow: 0 2px 4px rgba(0,0,0,0.04);
        text-align: center;
    }
    .metric-value {
        font-size: 24px;
        font-weight: 700;
        color: #1f77b4;
        margin-top: 4px;
    }
    .metric-label {
        font-size: 13px;
        font-weight: 600;
        color: #6c757d;
        text-transform: uppercase;
    }
    .insight-box {
        background-color: #e8f4fd;
        border-left: 5px solid #1f77b4;
        padding: 14px 18px;
        border-radius: 4px;
        margin-bottom: 20px;
        font-size: 14px;
        color: #1a202c;
    }
    /* Tab bar styling */
    button[data-baseweb="tab"] {
        font-size: 16px !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Hàm định dạng an toàn tránh crash khi giá trị bị NaN / None
def fmt_int(val, default=0) -> str:
    if val is None or pd.isna(val):
        return f"{default:,}"
    try:
        return f"{int(val):,}"
    except Exception:
        return f"{default:,}"


def fmt_currency(val, default=0.0) -> str:
    if val is None or pd.isna(val):
        return f"R$ {default:,.2f}"
    try:
        return f"R$ {float(val):,.2f}"
    except Exception:
        return f"R$ {default:,.2f}"


def fmt_percent(val, default=0.0) -> str:
    if val is None or pd.isna(val):
        return f"{default:.2f}%"
    try:
        return f"{float(val):.2f}%"
    except Exception:
        return f"{default:.2f}%"


# ------------------------------------------------------------------------------
# 2. CACHING TRUY VẤN DỮ LIỆU QUA STREAMLIT
# ------------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_filters():
    return get_filter_options()


@st.cache_data(ttl=600)
def load_sales(year, category):
    return query_sales_overview(selected_year=year, selected_category=category)


@st.cache_data(ttl=600)
def load_logistics(year, state):
    return query_logistics_overview(selected_year=year, selected_state=state)


@st.cache_data(ttl=600)
def load_retention(state):
    return query_retention_overview(selected_state=state)


filters = load_filters()

# ------------------------------------------------------------------------------
# 3. SIDEBAR: THÔNG TIN DỰ ÁN & KIẾN TRÚC HỆ THỐNG
# ------------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/data-configuration.png", width=64)
    st.title("Hệ Thống BI Đa Chiều")
    st.caption("BTL Big Data & Business Intelligence (CO4033) — Đề tài 17")
    st.markdown("---")

    st.markdown("### 🏛️ Kiến Trúc Hệ Thống")
    st.markdown(
        """
        * **Mô hình:** Medallion Lakehouse
          * 🥉 **Bronze:** Raw Multiplied CSVs
          * 🥈 **Silver:** Kimball Star Schema Parquet
          * 🥇 **Gold:** Flat Aggregated Data Marts
        * **Engine Phân Tích:** DuckDB In-Memory OLAP
        * **Trực quan:** Streamlit & Plotly
        """
    )
    st.markdown("---")
    st.markdown("### 📌 Hướng Dẫn Sử Dụng")
    st.info(
        "Mỗi tab chuyên đề bên phải được trang bị **thanh bộ lọc riêng biệt** ở trên đầu. "
        "Bộ lọc sẽ tác động tức thì vào các biểu đồ và chỉ số KPI của chuyên đề đó."
    )


# ------------------------------------------------------------------------------
# 4. TIÊU ĐỀ CHÍNH & CÁC TAB CHUYÊN ĐỀ BI
# ------------------------------------------------------------------------------
st.title("📊 Bảng Điều Khiển Quản Trị Doanh Nghiệp E-Commerce")
st.caption("Hệ thống thông minh hỗ trợ ra quyết định kinh doanh (Decision Support System) trên tập dữ liệu thương mại điện tử Olist.")

# 3 Tab điều hướng hiện đại đặt ngang trên đầu trang
tab_sales, tab_logistics, tab_retention = st.tabs([
    "📈 1. Doanh Số & Tăng Trưởng (Sales)",
    "🚚 2. Vận Hành & Chuỗi Cung Ứng (Logistics)",
    "👥 3. Khách Hàng & Giữ Chân (Retention)",
])


# ==============================================================================
# TAB 1: DOANH SỐ & TĂNG TRƯỞNG (SALES EXECUTIVE)
# ==============================================================================
with tab_sales:
    st.subheader("Phân Tích Doanh Thu & Cơ Cấu Ngành Hàng")

    # Thanh bộ lọc cục bộ cho Tab 1
    with st.container(border=True):
        col_f1, col_f2 = st.columns([1, 2])
        with col_f1:
            year_opts = ["Tất cả"] + [str(y) for y in filters["years"]]
            s_year_str = st.selectbox("📅 Năm đặt hàng:", options=year_opts, index=0, key="sales_year_filter")
            sales_year = int(s_year_str) if s_year_str != "Tất cả" else None
        with col_f2:
            cat_opts = ["Tất cả"] + filters["categories"]
            sales_category = st.selectbox("📦 Ngành hàng sản phẩm:", options=cat_opts, index=0, key="sales_cat_filter")

    sales_data = load_sales(sales_year, sales_category)
    kpis = sales_data["kpis"]

    # 4 Thẻ KPI Doanh số
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tổng GMV Toàn Sàn</div>
                <div class="metric-value">{fmt_currency(kpis['total_gmv'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tổng Đơn Hàng</div>
                <div class="metric-value">{fmt_int(kpis['total_orders'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Giá Trị TB Đơn (AOV)</div>
                <div class="metric-value">{fmt_currency(kpis['avg_order_value'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tổng Số Món Đã Bán</div>
                <div class="metric-value">{fmt_int(kpis['total_items'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")

    st.markdown(
        """
        <div class="insight-box">
            <b>💡 Hiện trạng dữ liệu (Data Finding):</b> Doanh thu toàn sàn phụ thuộc lớn vào <b>Top 10 ngành hàng chủ lực</b> (chiếm hơn 50% tổng GMV, dẫn đầu bởi Bed-Bath-Table, Health-Beauty và Sports-Leisure). Tuy nhiên, cước vận chuyển trung bình chiếm tới <b>~14 - 17% tổng giá trị đơn hàng</b>, tạo rào cản từ bỏ giỏ hàng (Cart Abandonment).
            <br/><b>📌 Hành động đề xuất (Action):</b> Triển khai gói <i>"Freeship Xtra / Trợ giá cước đơn gộp"</i> khi giá trị đơn hàng đạt từ R$ 150 hoặc khi mua từ 2 sản phẩm trở lên thuộc các ngành hàng chủ lực.
            <br/><b>⚖️ Phân tích Rủi ro & Đánh đổi (Trade-offs):</b> Doanh nghiệp chịu giảm nhẹ biên lợi nhuận ngắn hạn trên mỗi đơn để kích thích tăng trưởng <b>Giá trị trung bình đơn (AOV)</b> và số lượng sản phẩm bán ra trên mỗi đơn hàng (items_per_order).
        </div>
        """,
        unsafe_allow_html=True,
    )

    trend_df = sales_data["monthly_trend"]
    if not trend_df.empty and kpis["total_orders"] > 0:
        col_chart1, col_chart2 = st.columns([3, 2])

        with col_chart1:
            st.markdown("##### 📉 Xu Hướng Tăng Trưởng GMV Theo Tháng")
            fig_trend = px.area(
                trend_df,
                x="period",
                y="gmv",
                labels={"period": "Giai đoạn (Tháng)", "gmv": "Tổng GMV (R$)"},
                markers=True,
                color_discrete_sequence=["#1f77b4"],
            )
            fig_trend.update_layout(hovermode="x unified", margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_trend, use_container_width=True)

        with col_chart2:
            st.markdown("##### 🏆 Top 10 Ngành Hàng Đạt GMV Cao Nhất")
            cat_df = sales_data["top_categories"]
            fig_cat = px.bar(
                cat_df,
                x="gmv",
                y="category",
                orientation="h",
                labels={"gmv": "GMV (R$)", "category": "Ngành hàng"},
                color="gmv",
                color_continuous_scale="Blues",
            )
            fig_cat.update_layout(yaxis=dict(autorange="reversed"), margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.warning("⚠️ Không tìm thấy dữ liệu doanh số phù hợp với bộ lọc hiện tại.")


# ==============================================================================
# TAB 2: VẬN HÀNH & CHUỖI CUNG ỨNG (LOGISTICS & CSAT)
# ==============================================================================
with tab_logistics:
    st.subheader("Hiệu Suất Giao Hàng & Điểm Hài Lòng Khách Hàng (CSAT)")

    # Thanh bộ lọc cục bộ cho Tab 2
    with st.container(border=True):
        col_f1, col_f2 = st.columns([1, 1])
        with col_f1:
            year_opts = ["Tất cả"] + [str(y) for y in filters["years"]]
            l_year_str = st.selectbox("📅 Năm giao hàng:", options=year_opts, index=0, key="logistics_year_filter")
            logistics_year = int(l_year_str) if l_year_str != "Tất cả" else None
        with col_f2:
            state_opts = ["Tất cả"] + filters["states"]
            logistics_state = st.selectbox("📍 Bang khách hàng (Brazil):", options=state_opts, index=0, key="logistics_state_filter")

    logistics_data = load_logistics(logistics_year, logistics_state)
    l_kpis = logistics_data["kpis"]

    # 4 Thẻ KPI Logistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tỷ Lệ Giao Đúng Hạn</div>
                <div class="metric-value">{fmt_percent(l_kpis['on_time_rate'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        csat_val = float(l_kpis['avg_csat']) if not pd.isna(l_kpis['avg_csat']) else 0.0
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Điểm Đánh Giá CSAT TB</div>
                <div class="metric-value">{csat_val:.2f} / 5.0 ⭐</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Cước Vận Chuyển TB</div>
                <div class="metric-value">{fmt_currency(l_kpis['avg_freight'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tổng Đơn Hàng Đã Giao</div>
                <div class="metric-value">{fmt_int(l_kpis['total_delivered'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")

    st.markdown(
        """
        <div class="insight-box">
            <b>💡 Hiện trạng dữ liệu (Data Finding):</b> Tồn tại sự bất cân xứng địa lý nghiêm trọng giữa <b>Vùng Đông Nam (São Paulo - SP, Rio de Janeiro - RJ)</b> với <b>Vùng Đông Bắc / Bắc (Maranhão - MA, Ceará - CE, Alagoas - AL)</b>. Thời gian giao hàng đến vùng Đông Bắc kéo dài > 20 ngày, tỷ lệ trễ hạn cao gấp đôi vùng Đông Nam, khiến điểm CSAT sụt giảm mạnh (< 3.8 sao).
            <br/><b>📌 Hành động đề xuất (Action):</b> Thiết lập các <b>Trung tâm hoàn tất đơn hàng (Fulfillment Hubs) vệ tinh</b> tại Salvador hoặc Recife, kết hợp ký kết đối tác vận chuyển nội địa chặng cuối (Last-mile Delivery) tại vùng Đông Bắc.
            <br/><b>⚖️ Phân tích Rủi ro & Đánh đổi (Trade-offs):</b> Doanh nghiệp phải đầu tư chi phí hạ tầng kho bãi ban đầu (CAPEX/OPEX), nhưng sẽ giảm thiểu hơn 40% tỷ lệ hủy đơn do trễ hạn và bảo vệ uy tín thương hiệu lâu dài.
        </div>
        """,
        unsafe_allow_html=True,
    )

    state_df = logistics_data["state_breakdown"]
    if not state_df.empty and l_kpis["total_delivered"] > 0:
        col_geo1, col_geo2 = st.columns(2)

        with col_geo1:
            st.markdown(f"##### 📍 Tỷ Lệ Giao Đúng Hạn Theo 27 Bang {f'(Năm {logistics_year})' if logistics_year else ''}")
            fig_state_rate = px.bar(
                state_df.sort_values(by="on_time_rate", ascending=True),
                x="on_time_rate",
                y="customer_state",
                orientation="h",
                labels={"on_time_rate": "Tỷ lệ đúng hạn (%)", "customer_state": "Bang"},
                color="on_time_rate",
                color_continuous_scale="RdYlGn",
            )
            fig_state_rate.update_layout(margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_state_rate, use_container_width=True)

        with col_geo2:
            st.markdown("##### ⚖️ Tương Quan: Cước Phí Vận Chuyển vs CSAT")
            fig_scatter = px.scatter(
                state_df,
                x="avg_freight",
                y="avg_csat",
                size="total_orders",
                color="on_time_rate",
                hover_name="customer_state",
                labels={
                    "avg_freight": "Cước phí vận chuyển TB (R$)",
                    "avg_csat": "Điểm CSAT TB (1-5)",
                    "on_time_rate": "Tỷ lệ đúng hạn (%)",
                },
                color_continuous_scale="Viridis",
            )
            fig_scatter.update_layout(margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.warning(f"⚠️ Không tìm thấy đơn hàng giao thành công nào phù hợp với bộ lọc: Năm = {logistics_year or 'Tất cả'}, Bang = {logistics_state}.")


# ==============================================================================
# TAB 3: KHÁCH HÀNG & TỶ LỆ GIỮ CHÂN (CUSTOMER RETENTION)
# ==============================================================================
with tab_retention:
    st.subheader("Hành Vi Khách Hàng & Phân Tích Tần Suất Mua Lại")

    # Thanh bộ lọc cục bộ cho Tab 3
    with st.container(border=True):
        col_f1 = st.columns([1, 2])[0]
        with col_f1:
            state_opts = ["Tất cả"] + filters["states"]
            retention_state = st.selectbox("📍 Bang khách hàng (Brazil):", options=state_opts, index=0, key="retention_state_filter")

    retention_data = load_retention(retention_state)
    r_kpis = retention_data["kpis"]

    # 4 Thẻ KPI Retention
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tổng Khách Hàng Duy Nhất</div>
                <div class="metric-value">{fmt_int(r_kpis['total_customers'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Khách Mua Lại (Repeat)</div>
                <div class="metric-value">{fmt_int(r_kpis['repeat_customers'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tỷ Lệ Mua Lại (Retention)</div>
                <div class="metric-value">{fmt_percent(r_kpis['repeat_rate'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Tỷ Trọng GMV Khách Mua Lại</div>
                <div class="metric-value">{fmt_percent(r_kpis['repeat_spend_share'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")

    st.markdown(
        """
        <div class="insight-box">
            <b>💡 Hiện trạng dữ liệu (Data Finding):</b> Tỷ lệ mua lại trên sàn Olist hiện ở mức rất thấp (<b>~3.0%</b>), với hơn <b>95% khách hàng chỉ mua đúng 1 lần duy nhất</b> rồi rời bỏ. Chi phí thu hút khách hàng mới (CAC) ngày càng đắt đỏ nhưng giá trị vòng đời khách hàng (LTV) hầu như chưa được khai thác.
            <br/><b>📌 Hành động đề xuất (Action):</b> Tự động kích hoạt chiến dịch <b>Voucher giảm giá tái mua hàng (10-15%)</b> gửi qua Email/SMS/App trong vòng <b>30 ngày</b> kể từ khi đơn hàng đầu tiên được giao thành công; đồng thời xây dựng chương trình tích điểm khách hàng thân thiết (Loyalty Program).
            <br/><b>⚖️ Phân tích Rủi ro & Đánh đổi (Trade-offs):</b> Ngân sách khuyến mãi (discount burn) cho khách hàng cũ có chi phí thấp hơn 5-7 lần so với chi phí quảng cáo tìm khách hàng mới, giúp cải thiện tỷ số LTV/CAC và nâng tỷ trọng đóng góp GMV của khách hàng trung thành lên trên 15%.
        </div>
        """,
        unsafe_allow_html=True,
    )

    segment_df = retention_data["segment_comparison"]
    bucket_df = retention_data["bucket_distribution"]

    if not segment_df.empty and r_kpis["total_customers"] > 0:
        col_ret1, col_ret2 = st.columns(2)

        with col_ret1:
            st.markdown("##### 🍩 Cơ Cấu Khách Hàng (Mới vs Mua Lại)")
            fig_donut = px.pie(
                segment_df,
                values="total_customers",
                names="customer_segment",
                hole=0.45,
                color="customer_segment",
                color_discrete_map={"One-time Buyer": "#aec7e8", "Repeat Buyer": "#1f77b4"},
            )
            fig_donut.update_traces(textposition="inside", textinfo="percent+label")
            fig_donut.update_layout(margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_ret2:
            st.markdown("##### 📦 Phân Bố Số Lượng Đơn Đặt Trên Mỗi Khách")
            fig_bucket = px.bar(
                bucket_df,
                x="bucket_label",
                y="customer_count",
                labels={"bucket_label": "Tần suất đặt hàng", "customer_count": "Số lượng khách hàng"},
                color="customer_count",
                color_continuous_scale="Purples",
                text_auto=True,
            )
            fig_bucket.update_layout(margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_bucket, use_container_width=True)
    else:
        st.warning(f"⚠️ Không tìm thấy dữ liệu khách hàng cho Bang: {retention_state}.")
