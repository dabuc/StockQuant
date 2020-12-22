"""每日指标
"""

from stockquant.util.dataconvert import convert_to_bscode
from stockquant.odl.tushare.util import extract_data
from datetime import datetime as dtime
from stockquant.util.stringhelper import TaskEnum
from stockquant.util.models import TaskTable
from stockquant.odl.models import TS_Daily_Basic, TS_TradeCal
from stockquant.util.database import engine, session_scope
import tushare as ts
from stockquant.settings import CQ_Config
from dateutil.parser import parse
from stockquant.util import logger
from sqlalchemy import distinct, and_

_logger = logger.Logger(__name__).get_log()


def update_task():
    """
    更新任务表
    """
    # 首先删除历史任务
    TaskTable.del_with_task(TaskEnum.TS更新每日指标)

    with session_scope() as sm:
        cte1 = (
            sm.query(distinct(TS_TradeCal.date).label("date"))
            .filter(and_(TS_TradeCal.is_open == True, TS_TradeCal.date <= dtime.now().date()))  # noqa
            .cte("cte1")
        )

        cte2 = sm.query(distinct(TS_Daily_Basic.trade_date).label("trade_date")).cte("cte2")

        query = (
            sm.query(cte1.c.date)
            .join(cte2, cte1.c.date == cte2.c.trade_date, isouter=True)
            .filter(cte2.c.trade_date == None)  # noqa
        )
        trade_dates = query.all()

        tasklist = []
        for c in trade_dates:
            tasktable = TaskTable(
                task=TaskEnum.TS更新每日指标.value,
                task_name=TaskEnum.TS更新每日指标.name,
                ts_code="按日期更新",
                bs_code="按日期更新",
                begin_date=c.date,
                end_date=c.date,
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(trade_dates)))


def _parse_data(dic: dict):
    """
    解析数据，并保存
    """

    content = dic["result"]
    task_date = dic["task_date"]

    table_name = TS_Daily_Basic.__tablename__

    if content.empty:
        return

    try:
        content["trade_date"] = [parse(x).date() for x in content.trade_date]
        content["bs_code"] = [convert_to_bscode(x) for x in content.ts_code]
        content.to_sql(table_name, engine, schema=CQ_Config.DB_SCHEMA, if_exists="append", index=False)
    except Exception as e:
        _logger.error("{}-每日指标更新出错/{}".format(task_date, repr(e)))


def update_daily_basic():
    """
    更新每日指标
    """
    pro_api = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    fields = "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,\
    dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
    pro_api_func = pro_api.daily_basic
    extract_data(TaskEnum.TS更新每日指标, pro_api_func, {"fields": fields}, _parse_data, {}, "每日指标")
