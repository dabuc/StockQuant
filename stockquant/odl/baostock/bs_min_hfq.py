from stockquant.odl.models import BS_m60_hfq
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.baostock.util import query_history_k_data_plus
import pandas as pd
from stockquant.util.database import engine
from stockquant.util import logger
from stockquant.odl.baostock import util

_logger = logger.Logger(__name__).get_log()

task = TaskEnum.BS60分钟K线后复权数据


def update_task():
    """
    更新任务表
    """
    util.update_task(BS_m60_hfq, task, type=1)


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

    table_name = BS_m60_hfq.__tablename__

    try:
        content["date"] = pd.to_datetime(content["date"], format="%Y-%m-%d")
        content["time"] = pd.to_datetime(content["time"], format="%Y%m%d%H%M%S%f")
        # content['code'] =
        content["open"] = pd.to_numeric(content["open"], errors="coerce")
        content["high"] = pd.to_numeric(content["high"], errors="coerce")
        content["low"] = pd.to_numeric(content["low"], errors="coerce")
        content["close"] = pd.to_numeric(content["close"], errors="coerce")
        content["volume"] = pd.to_numeric(content["volume"], errors="coerce")
        content["amount"] = pd.to_numeric(content["amount"], errors="coerce")
        # content['adjustflag'] =

        content.to_sql(table_name, engine, if_exists="append", index=False)

    except Exception as e:  # traceback.format_exc(1)
        _logger.error("{}保存出错/{}".format(bs_code, repr(e)))


def get_min_hfq():
    """
    docstring
    """
    query_history_k_data_plus(task, "60", "1", _load_data)
