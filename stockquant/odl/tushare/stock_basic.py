# -*- coding: utf-8 -*-
"""股票列表"""
from stockquant.odl.models import TS_Stock_Basic
from stockquant.util.database import engine
from stockquant.settings import CQ_Config
import tushare as ts
import pandas as pd


def convert_to_bscode(ts_code):
    """
    ts_code 转换成 bs_code
    """
    b = ts_code.split(".")
    bs_code = "{}.{}".format(b[1].lower(), b[0])
    return bs_code


def get_stock_basic():
    """
    获取股票列表
    """

    # 清空原有数据
    TS_Stock_Basic.clear_table()

    pro = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    # 查询当前所有正常上市交易的股票列表

    fields = "ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,\
    list_date,delist_date,is_hs"

    rs_L = pro.stock_basic(exchange="", list_status="L", fields=fields)
    rs_D = pro.stock_basic(exchange="", list_status="D", fields=fields)
    rs_P = pro.stock_basic(exchange="", list_status="P", fields=fields)

    result = pd.concat([rs_L, rs_D, rs_P])

    result["list_date"] = pd.to_datetime(result["list_date"])
    result["delist_date"] = pd.to_datetime(result["delist_date"])

    result["bs_code"] = result["ts_code"].apply(convert_to_bscode)

    result.to_sql("odl_ts_stock_basic", engine, schema=CQ_Config.DB_SCHEMA, if_exists="append", index=False)


if __name__ == "__main__":
    pass
