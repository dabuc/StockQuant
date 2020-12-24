import datetime
from stockquant.odl.tushare.util import extract_data
from stockquant.settings import CQ_Config
from stockquant.odl.models import TS_TradeCal, TS_stk_limit
from stockquant.util.database import session_scope, engine
from stockquant.util.models import TaskTable
from stockquant.util.stringhelper import TaskEnum
from sqlalchemy import distinct, and_
from datetime import datetime as dtime
from stockquant.util import logger
import tushare as ts
import pandas as pd

_logger = logger.Logger(__name__).get_log()


def update_task():
    """
    更新任务列表
    """

    # 首先删除历史任务
    TaskTable.del_with_task(TaskEnum.TS每日涨跌停价格)

    with session_scope() as sm:
        query = sm.query(distinct(TS_TradeCal.date).label("date")).filter(
            and_(
                TS_TradeCal.is_open == True,  # noqa
                TS_TradeCal.date >= dtime(2007, 1, 1),
                TS_TradeCal.date <= dtime.now().date(),
            )
        )
        if CQ_Config.IDB_DEBUG == "1":  # 如果是测试环境
            query = query.filter(TS_TradeCal.date >= dtime.now().date() - datetime.timedelta(days=10))
        cte1 = query.cte("cte1")

        cte2 = sm.query(distinct(TS_stk_limit.trade_date).label("trade_date")).cte("cte2")

        query = (
            sm.query(cte1.c.date)
            .join(cte2, cte1.c.date == cte2.c.trade_date, isouter=True)
            .filter(cte2.c.trade_date == None)  # noqa
        )
        trade_dates = query.all()

        tasklist = []
        for c in trade_dates:
            tasktable = TaskTable(
                task=TaskEnum.TS每日涨跌停价格.value,
                task_name=TaskEnum.TS每日涨跌停价格.name,
                ts_code="按日期更新",
                bs_code="按日期更新",
                begin_date=c.date,
                end_date=c.date,
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(trade_dates)))


def get_stk_limit():
    """
    获取每日涨跌停价格
    """
    pro_api = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    pro_api_func = pro_api.stk_limit  # 获取单日全部股票数据涨跌停价格
    extract_data(TaskEnum.TS每日涨跌停价格, pro_api_func, {}, _load_data, {}, "每日涨跌停价格", sleep_time=0.8)


def _load_data(dic: dict):
    """
    做一些简单转换后，加载数据到数据库
    """
    content = dic["result"]
    task_date = dic["task_date"]

    table_name = TS_stk_limit.__tablename__

    if content.empty:
        return

    try:
        content["trade_date"] = pd.to_datetime(content["trade_date"], format="%Y-%m-%d")
        content["up_limit"] = pd.to_numeric(content["up_limit"], errors="coerce")
        content["down_limit"] = pd.to_numeric(content["down_limit"], errors="coerce")
        content.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        _logger.error("{}-日线行情保存出错/{}".format(task_date, repr(e)))
