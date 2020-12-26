# -*- coding: utf-8 -*-
"""
指数基本信息
"""
import tushare as ts
from stockquant.settings import CQ_Config
import pandas as pd
from stockquant.odl.models import TS_Index_Basic
from stockquant.util.database import engine


def update_index_basic():
    """
    获取TS指数基础信息
    """
    pro = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    market = ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"]
    dfs = []

    for item in market:
        df = pro.index_basic(market=item)
        dfs.append(df)

    c = pd.concat(dfs)
    c["base_date"] = pd.to_datetime(c["base_date"])
    c["list_date"] = pd.to_datetime(c["list_date"])

    c.to_sql(TS_Index_Basic.__tablename__, engine, if_exists="append", index=False)
