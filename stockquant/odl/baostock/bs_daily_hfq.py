from stockquant.util.taskmanage import create_bs_task
from stockquant.util.models import TaskTable
from stockquant.util.dataconvert import get_decimal_from_str, get_int_from_str
from stockquant.util import logger
from datetime import datetime as dtime
from stockquant.odl.models import BS_Daily_hfq, BS_Stock_Basic
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.baostock.util import query_history_k_data_plus
from stockquant.util.database import engine, session_scope
from sqlalchemy import func, or_, and_
import datetime
import pandas as pd

_logger = logger.Logger(__name__).get_log()


def update_task(reset: bool = False):
    """
    更新任务表
    """
    if reset:
        create_bs_task(TaskEnum.BS日线历史A股K线后复权数据)
        return

    # 删除历史任务记录
    TaskTable.del_with_task(TaskEnum.BS日线历史A股K线后复权数据)

    with session_scope() as sm:
        # 通过BS证券基本资料和A股K线后复权数据的每个股票的最新交易时间，查出所有需要更新的股票及更新时间
        cte = (
            sm.query(BS_Daily_hfq.code, func.max(BS_Daily_hfq.date).label("mx_date"))
            .group_by(BS_Daily_hfq.code)
            .cte("cte")
        )
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
                task=TaskEnum.BS日线历史A股K线后复权数据.value,
                task_name=TaskEnum.BS日线历史A股K线后复权数据.name,
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

    table_name = BS_Daily_hfq.__tablename__

    try:

        # content["date"] = [dtime.strptime(x, "%Y-%m-%d").date() for x in content["date"]]
        content["date"] = pd.to_datetime(content["date"], format="%Y-%m-%d")
        # content['code'] =
        content["open"] = [None if x == "" else get_decimal_from_str(x) for x in content["open"]]
        content["high"] = [None if x == "" else get_decimal_from_str(x) for x in content["high"]]
        content["low"] = [None if x == "" else get_decimal_from_str(x) for x in content["low"]]
        content["close"] = [None if x == "" else get_decimal_from_str(x) for x in content["close"]]
        content["preclose"] = [None if x == "" else get_decimal_from_str(x) for x in content["preclose"]]
        content["volume"] = [None if x == "" else get_int_from_str(x) for x in content["volume"]]
        content["amount"] = [None if x == "" else get_decimal_from_str(x) for x in content["amount"]]
        # content['adjustflag'] =
        content["turn"] = [None if x == "" else get_decimal_from_str(x) for x in content["turn"]]
        content["tradestatus"] = [None if x == "" else bool(get_int_from_str(x)) for x in content["tradestatus"]]
        content["pctChg"] = [None if x == "" else get_decimal_from_str(x) for x in content["pctChg"]]
        content["peTTM"] = [None if x == "" else get_decimal_from_str(x) for x in content["peTTM"]]
        content["psTTM"] = [None if x == "" else get_decimal_from_str(x) for x in content["psTTM"]]
        content["pcfNcfTTM"] = [None if x == "" else get_decimal_from_str(x) for x in content["pcfNcfTTM"]]
        content["pbMRQ"] = [None if x == "" else get_decimal_from_str(x) for x in content["pbMRQ"]]
        content["isST"] = [None if x == "" else bool(get_int_from_str(x)) for x in content["isST"]]

        content.to_sql(table_name, engine, if_exists="append", index=False)

    except Exception as e:  # traceback.format_exc(1)
        _logger.error("{}保存出错/{}".format(bs_code, repr(e)))


def get_daily_hfq():
    """
    按照任务表，获取BS日线后复权行情数据
    """

    query_history_k_data_plus(TaskEnum.BS日线历史A股K线后复权数据, "d", "1", _load_data)
