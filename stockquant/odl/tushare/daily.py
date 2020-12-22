# -*- coding: utf-8 -*-
"""
日线行情
"""
from stockquant.util.models import TaskTable
from stockquant.util import logger
from stockquant.odl.models import TS_Daily, TS_TradeCal
from stockquant.settings import CQ_Config
from stockquant.odl.tushare.util import extract_data
from stockquant.util.stringhelper import TaskEnum
import tushare as ts
from dateutil.parser import parse
from sqlalchemy import String, distinct, and_
from stockquant.util.database import engine, session_scope
from datetime import datetime as dtime

_logger = logger.Logger(__name__).get_log()


def update_task():
    """
    更新任务列表
    """

    # 首先删除历史任务
    TaskTable.del_with_task(TaskEnum.TS日线行情)

    with session_scope() as sm:
        cte1 = (
            sm.query(distinct(TS_TradeCal.date).label("date"))
            .filter(and_(TS_TradeCal.is_open == True, TS_TradeCal.date <= dtime.now().date()))  # noqa
            .cte("cte1")
        )

        cte2 = sm.query(distinct(TS_Daily.trade_date).label("trade_date")).cte("cte2")

        query = (
            sm.query(cte1.c.date)
            .join(cte2, cte1.c.date == cte2.c.trade_date, isouter=True)
            .filter(cte2.c.trade_date == None)  # noqa
        )
        trade_dates = query.all()

        tasklist = []
        for c in trade_dates:
            tasktable = TaskTable(
                task=TaskEnum.TS日线行情.value,
                task_name=TaskEnum.TS日线行情.name,
                ts_code="按日期更新",
                bs_code="按日期更新",
                begin_date=c.date,
                end_date=c.date,
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(trade_dates)))


def get_daily():
    """
    获取日线行情
    """

    pro_api = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    pro_api_func = pro_api.daily
    extract_data(TaskEnum.TS日线行情, pro_api_func, {}, _load_data, {}, "日线行情")


def _load_data(dic: dict):
    """
    做一些简单转换后，加载数据到数据库
    """

    content = dic["result"]
    task_date = dic["task_date"]

    table_name = TS_Daily.__tablename__

    if content.empty:
        return

    try:
        content["trade_date"] = [parse(x).date() for x in content.trade_date]
        dtype = {"ts_code": String(10)}

        content.to_sql(table_name, engine, schema=CQ_Config.DB_SCHEMA, if_exists="append", index=False, dtype=dtype)
    except Exception as e:
        _logger.error("{}-日线行情保存出错/{}".format(task_date, repr(e)))
