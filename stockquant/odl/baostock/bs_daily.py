from stockquant.util.models import TaskTable
from stockquant.util import logger
from datetime import datetime as dtime
from stockquant.odl.models import BS_Daily, BS_Stock_Basic
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.baostock.util import query_history_k_data_plus
from stockquant.util.database import engine, session_scope
from sqlalchemy import func, or_, and_
import datetime
import pandas as pd

_logger = logger.Logger(__name__).get_log()


def update_task():
    """
    更新任务表-BS日线历史A股K线数据
    """
    # 删除历史任务记录
    TaskTable.del_with_task(TaskEnum.BS日线历史A股K线数据)

    with session_scope() as sm:
        # 通过BS证券基本资料和A股K线数据的每个股票的最新交易时间，查出所有需要更新的股票及更新时间
        cte = sm.query(BS_Daily.code, func.max(BS_Daily.date).label("mx_date")).group_by(BS_Daily.code).cte("cte")
        query = sm.query(
            BS_Stock_Basic.code, BS_Stock_Basic.ts_code, BS_Stock_Basic.ipoDate, BS_Stock_Basic.outDate, cte.c.mx_date
        )
        query = query.join(cte, BS_Stock_Basic.code == cte.c.code, isouter=True)
        query = query.filter(
            or_(
                and_(BS_Stock_Basic.outDate == None, cte.c.mx_date < dtime.now().date()),  # noqa
                cte.c.mx_date == None,
                BS_Stock_Basic.outDate > cte.c.mx_date,
            )
        )

        codes = query.all()
        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=TaskEnum.BS日线历史A股K线数据.value,
                task_name=TaskEnum.BS日线历史A股K线数据.name,
                ts_code=c.ts_code,
                bs_code=c.code,
                begin_date=c.ipoDate if c.mx_date is None else c.mx_date + datetime.timedelta(days=1),
                end_date=c.outDate if c.outDate is not None else dtime.now().date(),
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


def _load_data(dic: dict):
    """
    docstring
    """
    content = dic["result"]
    bs_code = dic["bs_code"]
    # frequency = dic["frequency"]
    # adjustflag = dic["adjustflag"]

    if content.empty:
        return

    table_name = BS_Daily.__tablename__

    try:

        content["date"] = pd.to_datetime(content["date"], format="%Y-%m-%d")
        # content['code'] =
        content["open"] = pd.to_numeric(content["open"], errors="coerce")
        content["high"] = pd.to_numeric(content["high"], errors="coerce")
        content["low"] = pd.to_numeric(content["low"], errors="coerce")
        content["close"] = pd.to_numeric(content["close"], errors="coerce")
        content["preclose"] = pd.to_numeric(content["preclose"], errors="coerce")
        content["volume"] = pd.to_numeric(content["volume"], errors="coerce")
        content["amount"] = pd.to_numeric(content["amount"], errors="coerce")
        # content['adjustflag'] =
        content["turn"] = pd.to_numeric(content["turn"], errors="coerce")
        content["tradestatus"] = pd.to_numeric(content["tradestatus"], errors="coerce").astype(bool)
        content["pctChg"] = pd.to_numeric(content["pctChg"], errors="coerce")
        content["peTTM"] = pd.to_numeric(content["peTTM"], errors="coerce")
        content["psTTM"] = pd.to_numeric(content["psTTM"], errors="coerce")
        content["pcfNcfTTM"] = pd.to_numeric(content["pcfNcfTTM"], errors="coerce")
        content["pbMRQ"] = pd.to_numeric(content["pbMRQ"], errors="coerce")
        content["isST"] = pd.to_numeric(content["isST"], errors="coerce").astype(bool)

        content.to_sql(table_name, engine, if_exists="append", index=False)

    except Exception as e:  # traceback.format_exc(1)
        _logger.error("{}保存出错/{}".format(bs_code, repr(e)))


def get_daily():
    """
    按照任务表，获取BS日线后复权行情数据
    """

    query_history_k_data_plus(TaskEnum.BS日线历史A股K线数据, "d", "3", _load_data)
