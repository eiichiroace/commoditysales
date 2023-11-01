import os.path
from datetime import datetime, timedelta, date
import pandas as pd
import streamlit as st
from streamlit.logger import get_logger
from urllib.parse import urlparse, parse_qs

LOGGER = get_logger(__name__)

st.set_page_config(
    page_title="重点产品销量趋势",
    layout="wide"
)


@st.cache_resource()
def initial_connection():
    # 连接数据库
    conn = st.connection("mydb", type="sql", autocommit=True)
    return conn


conn = initial_connection()
query_key_product_sales = f"""
     select * from kj_data.KeyProductSales;
     """
query_product_message = """
                select * from kj_data.product_message;
            """


@st.cache_data(ttl=10, show_spinner="Fetching data from API...")
def select_data(sql):
    # 查询数据
    result = conn.query(sql, ttl=10)
    if result is not None:
        df = result
        return df
    else:
        st.warning("查询结果为空")
        return 0


def extract_country_and_product_from_link(link):
    parsed = urlparse(link)
    params = parse_qs(parsed.query)
    country = params.get('region', ['US'])[0]
    product_id = parsed.path.split('/')[-1]
    return country, product_id


def load_mapping_from_xlsx(xlsx_path):
    absolute_path = os.path.abspath(xlsx_path)
    # print(f"Loading data from: {absolute_path}")
    df = pd.read_excel(absolute_path)
    product_mapping = df.set_index(df['pid'].astype(str))['product_name'].to_dict()
    # print(product_mapping)
    return product_mapping


CURRENCY_CONVERSION_RATES = {
    'RM': 0.24,  # Conversion rate from MYR to USD (example rate)马来西亚 马币
    'S$': 0.74,  # Conversion rate from SGD to USD (example rate)新加坡 新币
    '$': 1,  # Assume USD as base currency美元
    '฿': 0.03,  # Conversion rate from THB to USD (example rate) 泰铢
    '₫': 0.000043,  # Conversion rate from VND to USD (example rate) 越南盾
    '₱': 0.020  # Conversion rate from PHP to USD (example rate) 比索
}

# Sort currency symbols by length, longest first
CURRENCY_SYMBOLS = sorted(CURRENCY_CONVERSION_RATES.keys(), key=len, reverse=True)


def get_currency_conversion_rate(symbol):
    return CURRENCY_CONVERSION_RATES.get(symbol, 1)  # Default to 1 if symbol is not recognized


def lowest_price_from_string(price_str):
    try:
        # Identify currency symbol
        for symbol in CURRENCY_SYMBOLS:
            if symbol in price_str:
                conversion_rate = get_currency_conversion_rate(symbol)
                break
        else:
            symbol = ''
            conversion_rate = 1  # Default to 1 if no symbol is found

        # Process price range
        price_ranges = price_str.replace(symbol, "").split('-')
        lowest_price = min([float(p) for p in price_ranges])  # Find the lowest price
        return lowest_price * conversion_rate  # Convert to USD
    except Exception as e:
        return None


def calculate_rolling_sales(group):
    # 将 'create_at' 列转换为日期时间类型
    group['create_at'] = pd.to_datetime(group['create_at'])
    # 确保每个分组内部按照 'create_at' 排序
    group = group.sort_values('create_at')
    # 确保组的索引是时间序列
    group = group.set_index('create_at')

    # 应用滚动窗口计算
    group['Recent 24h Sales Growth'] = group['Sales Growth'].rolling('24h').sum()
    # 重置索引
    group = group.reset_index()
    return group


def handle_data():
    df_key_product_sales = select_data(query_key_product_sales)
    df_product_message = select_data(query_product_message)

    # 数据类型转换和排序
    df_key_product_sales['create_at'] = pd.to_datetime(df_key_product_sales['create_at'])
    df_key_product_sales['sales'] = pd.to_numeric(df_key_product_sales['sales'])
    # df_key_product_sales['country'], df_key_product_sales['product_id'] = zip(
    #     *df_key_product_sales['product_link'].apply(extract_country_and_product_from_link))

    df_key_product_sales = df_key_product_sales.sort_values(['product_link', 'create_at'])
    df_key_product_sales['Sales Growth'] = df_key_product_sales.groupby('product_link')['sales'].diff().fillna(0)
    # # 提取最低价格
    df_key_product_sales['lowest_price'] = df_key_product_sales['price'].apply(lowest_price_from_string)
    # 计算24小时内销量和销量增长
    df_key_product_sales = df_key_product_sales.groupby('product_link').apply(calculate_rolling_sales).reset_index(
        drop=True)

    df_product_message['product_link'] = df_product_message.apply(
        lambda row: f"https://shop.tiktok.com/view/product/{row['pid']}?region=US&locale=en" if pd.isna(
            row['product_link']) else row['product_link'], axis=1)

    # 合并表1和表2
    df_merged = pd.merge(df_product_message, df_key_product_sales, on='product_link', how='left')

    # 计算24小时GMV
    df_merged['24h GMV'] = df_merged['lowest_price'] * df_merged['Recent 24h Sales Growth']
    # 首先按照时间降序排序，这样最新的条目会排在前面
    df_sorted = df_merged.sort_values(by='create_at', ascending=False)  # 确保使用正确的时间列名

    # 然后使用 drop_duplicates 保留每个 'product_link' 的第一个条目，即最新的条目
    df_unique = df_sorted.drop_duplicates(subset='product_link', keep='first')

    # 现在 df_unique 包含了每个唯一 product_link 的最新条目
    # 返回处理后的数据
    return df_unique

    # # 从链接中取出国家属性
    # df['country'], df['product_id'] = zip(*df['product_link'].apply(extract_country_and_product_from_link))
    #
    # product_name_mapping = load_mapping_from_xlsx('pages/product_message1.xlsx')
    # df['product'] = df['product_id'].map(lambda x: product_name_mapping.get(x, "Unknown Product"))
    #
    # # 提取最低价格
    # df['lowest_price'] = df['price'].apply(lowest_price_from_string)
    #
    # # 假设df已经按照create_at排序
    # # 计算最近24小时的销量增长
    # df = df.groupby('product_link').apply(calculate_rolling_sales).reset_index(drop=True)
    #
    # # 计算GMV
    # df['GMV Growth'] = df['Recent 24h Sales Growth'] * df['lowest_price']
    # # 之后的每个时间点的GMV则为之前的GMV + 当期的GMV增长
    # # df['GMV'] = df.groupby('product_link')['GMV Growth'].cumsum()
    #
    # # Sidebar filters
    # selected_countries = st.sidebar.multiselect("选择国家", df['country'].unique(), default=df['country'].unique())
    # # selected_products = st.sidebar.multiselect("选择产品", df['product'].unique(), default=df['product'].unique())
    #
    # filtered_df = df[df['country'].isin(selected_countries)]  # & df['product'].isin(selected_products)]
    # st.write(filtered_df)
    # st.dataframe(filtered_df)
    # # 获取当前时间和24小时前的时间
    # now = datetime.now()
    # last_24_hours = now - timedelta(hours=24)
    #
    # # 过滤出最近24小时内的数据
    # df_last_24_hours = filtered_df[filtered_df['create_at'] >= last_24_hours]
    #
    # # 计算每个产品的总销量
    # total_sales_per_product = df_last_24_hours.groupby('product_link')['sales'].sum().sort_values(ascending=False)
    # # 获取销量排名前100的产品链接
    # top_100_product_links = total_sales_per_product.head(100).index
    # # 筛选销量排名前100的产品数据
    # df_top_100 = df[df['product_link'].isin(top_100_product_links)]
    # return df_top_100


# column_configuration = {
#     "name": st.column_config.TextColumn(
#         "产品名称", help="The name of the product", max_chars=100
#     ),
#     "picture": st.column_config.ImageColumn("产品图", help="The product_image"),
#     "product_link": st.column_config.LinkColumn(
#         "产品链接", help="The homepage of the product"
#     ),
#     "daily_sales_growth": st.column_config.NumberColumn("24小时销量增长", format="%.2f"),
#     "price": st.column_config.TextColumn(
#         "单价", help="产品链接主页价格"),
#     "Sales Growth": st.column_config.LineChartColumn(
#         "24小时销量增长",
#         width="large"
#     ),
#     "GMV Growth": st.column_config.LineChartColumn(
#         "24小时GMV增长",
#         width="medium"
#     ),
#     "total_sales_growth": st.column_config.TextColumn(
#         "总销量增长",
#         help="总销量增长"  # validate="^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$",
#     ),
# }


def show_chart(df):
    st.title(':rainbow: 重点产品销量趋势 :rainbow:')

    # 配置展示列
    column_configuration = {
        "first_category": st.column_config.TextColumn("一级类目"),
        "product_name": st.column_config.TextColumn("产品名称"),
        "picture": st.column_config.ImageColumn("产品图片"),
        "product_link": st.column_config.LinkColumn("产品链接"),
        "Recent 24h Sales Growth": st.column_config.NumberColumn("24小时销量", format="%.2f"),
        "24h GMV": st.column_config.NumberColumn("24小时GMV", format="%.2f"),
        "price": st.column_config.TextColumn("客单价"),
        "country": st.column_config.TextColumn("国家")
    }
    # st.dataframe(df)
    # 仅展示需要的列
    display_columns = ['picture', 'first_category', 'product_name',  'Recent 24h Sales Growth',
                       '24h GMV', 'price', 'product_link', 'country']
    # 获取国家的唯一值列表
    unique_countries = df['country'].unique().tolist()

    # 创建选择框供用户选择
    selected_countries = st.multiselect('Select Countries:', unique_countries, default=unique_countries)

    # 根据用户选择筛选数据
    filtered_df = df[df['country'].isin(selected_countries)]

    st.data_editor(filtered_df[display_columns], column_config=column_configuration, use_container_width=True,
                   hide_index=True, num_rows="fixed", height=1500)


def run():
    df = handle_data()
    show_chart(df)


if __name__ == '__main__':
    run()
