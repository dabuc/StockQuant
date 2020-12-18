# -*- coding: utf-8 -*-
from enum import Enum, unique


@unique
class TaskEnum(Enum):
    """
    任务枚举
    """

    日线历史A股K线数据 = "d"
    周线历史A股K线数据 = "w"
    月线历史A股K线数据 = "m"
    T5分钟线历史A股K线数据 = "5"

    日线历史A股K线前复权数据 = "d-2"
    周线历史A股K线前复权数据 = "w-2"
    月线历史A股K线前复权数据 = "m-2"

    日线历史A股K线后复权数据 = "d-1"

    季频盈利能力 = "profit"

    BS日线历史A股K线后复权数据 = "bs_daily_hfq"
    BS计算指定日期往后N日涨跌幅 = "calc_later_n_pctChg"

    TS更新每日指标 = "daily_basic"
    TS日线行情 = "daily"
    TS复权因子 = "adj_factor"
