import pandas as pd
import streamlit as st
from streamlit.logger import get_logger
from urllib.parse import urlparse, parse_qs

LOGGER = get_logger(__name__)

st.set_page_config(
    page_title="重点产品销量趋势",
    layout="wide"
)


@st.cache_resource
def initial_connection():
    # 连接数据库
    conn = st.experimental_connection("mydb", type="sql", autocommit=True)
    return conn


conn = initial_connection()
query = f"""
     select * from kj_data.KeyProductSales;
     """


@st.cache_data(ttl=600, show_spinner="Fetching data from API...")
def select_data(sql):
    # 查询数据
    result = conn.query(sql, ttl=3600)
    if result is not None:
        df = result
        return df
    else:
        st.warning("查询结果为空")
        return 0


def extract_country_and_product_from_link(link):
    parsed = urlparse(link)
    params = parse_qs(parsed.query)
    country = params.get('region', [None])[0]
    product_id = parsed.path.split('/')[-1]
    return country, product_id


def handle_data(sql_query):
    df = select_data(sql_query)
    df['create_at'] = pd.to_datetime(df['create_at'])
    df['sales'] = pd.to_numeric(df['sales'])
    df.sort_values(['product_link', 'create_at'], inplace=False)

    # Extract country and product from link
    df['country'], df['product_id'] = zip(*df['product_link'].apply(extract_country_and_product_from_link))

    product_name_mapping = {
        "1729609825194445394": "口喷",
        "1729615303577733714": "牙膏",
        "1729652361741044306": "口喷",
        "1729565221372005095": "奶粉",
        "1729689196463557202": "舌苔啫喱",
        "1729609916326447698": "牙膏"
    }
    df['product'] = df['product_id'].map(product_name_mapping)

    # Sidebar filters
    selected_countries = st.sidebar.multiselect("选择国家", df['country'].unique(), default=df['country'].unique())
    selected_products = st.sidebar.multiselect("选择产品", df['product'].unique(), default=df['product'].unique())

    filtered_df = df[df['country'].isin(selected_countries) & df['product'].isin(selected_products)]

    return filtered_df


def show_chart(df):
    st.title(':rainbow[重点产品销量趋势]:rainbow:')

    # Calculate growth for each product link
    df['Growth'] = df.groupby('product_link')['sales'].diff().fillna(0)

    # Sort product links by total growth
    total_growth = df.groupby('product_link')['Growth'].sum().sort_values(ascending=False)
    
    for product_link in total_growth.index:
        product_df = df[df['product_link'] == product_link]
        product_name = product_df['product'].iloc[0]  # 获取产品名称
        link_label = "查看产品"
        markdown_link = f"[{link_label}]({product_link})"
        st.markdown(f'{product_name} - 销售量: {int(total_growth[product_link])} - {markdown_link}', unsafe_allow_html=True)
        
        # st.write(f'{product_name}Total Growth for {product_link}: {int(total_growth[product_link])}')
        col1, col2 = st.columns([3, 1])
        product_df.sort_values(['sales'], ascending=False, inplace=True)
        col2.write(product_df)
        col1.line_chart(product_df[['create_at', 'Growth']], x='create_at', y='Growth')


def run():
    df = handle_data(query)
    show_chart(df)


if __name__ == '__main__':
    run()
