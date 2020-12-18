from stockquant.odl.models import BS_SZ50_Stocks
import baostock as bs
import pandas as pd
from stockquant.util.database import engine


def get_sz50_stocks():
    """
    获取上证50成分股数据
    """
    # 删除数据
    BS_SZ50_Stocks.clear_table()

    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print("login respond error_code:" + lg.error_code)
    print("login respond  error_msg:" + lg.error_msg)

    # 获取上证50成分股
    rs = bs.query_sz50_stocks()
    print("query_sz50 error_code:" + rs.error_code)
    print("query_sz50  error_msg:" + rs.error_msg)

    # 打印结果集
    sz50_stocks = []
    while (rs.error_code == "0") & rs.next():
        # 获取一条记录，将记录合并在一起
        sz50_stocks.append(rs.get_row_data())
    result = pd.DataFrame(sz50_stocks, columns=rs.fields)

    result.to_sql("odl_bs_sz50_stocks", engine, if_exists="append", index=False)

    # 登出系统
    bs.logout()
