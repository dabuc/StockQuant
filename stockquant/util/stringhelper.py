# -*- coding: utf-8 -*-
from enum import Enum, unique


@unique
class TaskEnum(Enum):
    """
    任务枚举
    """

    季频盈利能力 = "profit"

    BS日线历史A股K线后复权数据 = "bs_daily_hfq"
    BS周线历史A股K线后复权数据 = "bs_weekly_hfq"
    BS日线历史A股K线数据 = "bs_daily"
    BS计算指定日期往后N日涨跌幅 = "bs_calc_later_n_pctChg"

    TS更新每日指标 = "ts_daily_basic"
    TS日线行情 = "ts_daily"
    TS复权因子 = "ts_adj_factor"
    TS每日涨跌停价格 = "ts_stk_limit"

    # -----------回测任务---------
    BS均线应用_两线法 = "two-EMA-win-rate"
