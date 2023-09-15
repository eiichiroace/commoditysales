import pandas as pd
import streamlit as st
from streamlit.logger import get_logger

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


@st.cache_data(ttl=3600, show_spinner="Fetching data from API...")
def select_data(sql):
    # 查询数据
    result = conn.query(sql, ttl=3600)

    # 检查查询结果是否为空
    if result is not None:
        # 查询结果不为空，可以直接使用查询结果进行进一步的操作
        df = result
        return df
        # 现在可以使用 DataFrame 'df' 进行进一步的操作
    else:
        # 查询结果为空，可以在这里添加相应的处理逻辑
        st.warning("查询结果为空")
        return 0


def handle_data(query):
    df = select_data(query)

    df['create_at'] = pd.to_datetime(df['create_at'])
    df['sales'] = pd.to_numeric(df['sales'])
    # 按 'VideoID' 列和 'Timestamp' 列对数据进行排序
    df.sort_values(['product_link', 'create_at'], inplace=False)

    # 创建Streamlit应用程序
    st.title(':rainbow[重点产品销量趋势]:rainbow:')

    # 获取所有不同视频的列表
    product_list = df['product_link'].unique()

    # 创建一个字典来存储产品的增长量
    growth_dict = {}
    p_list = st.sidebar.multiselect(
        "选择你的产品链接，可模糊搜索", product_list
    )
    return df, product_list, p_list, growth_dict


# 绘制所有视频的播放量增长折线图


def show_chart(pro_list, df, growth_dict):
    for product_id in pro_list:
        filtered_df = df[df['product_link'] == product_id].copy()
        filtered_df['Growth'] = filtered_df['sales'].diff().fillna(0)

        # 计算总增长量
        total_growth = filtered_df['Growth'].sum()

        # 存储总增长量到字典中
        growth_dict[product_id] = total_growth
        # 对字典按值（增长量）降序排序
    sorted_growth_dict = dict(sorted(growth_dict.items(), key=lambda item: item[1], reverse=True))
    # 遍历排好序的产品，并绘制增长量折线图
    for product_id in sorted_growth_dict.keys():
        filtered_df = df[df['product_link'] == product_id].copy()
        filtered_df['Growth'] = filtered_df['sales'].diff().fillna(0)

        # 对增长量列进行排序
        filtered_df.sort_values(by='Growth', ascending=False, inplace=True)

        # 移除 'product_link' 列和 '_id' 列
        filtered_df = filtered_df.drop(['product_link', '_id'], axis=1)

        # 显示增长量总和
        total_growth = filtered_df['Growth'].sum()
        st.write(f'Total Growth for {product_id}: {int(total_growth)}')
        col1, col2 = st.columns([3, 1])
        filtered_df.sort_values(['sales'], ascending=False, inplace=True)
        col2.write(filtered_df)
        # 绘制增长量折线图
        col1.line_chart(filtered_df[['create_at', 'Growth']], x='create_at', y='Growth')


def run():
    df, product_list, p_list, growth_dict = handle_data(query)
    if not p_list:
        show_chart(product_list, df, growth_dict)
    else:
        show_chart(p_list, df, growth_dict)


if __name__ == '__main__':
    run()
