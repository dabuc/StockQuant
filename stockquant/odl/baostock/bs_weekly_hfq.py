from stockquant.util import logger
from stockquant.odl.models import BS_Weekly_hfq
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.baostock.util import query_history_k_data_plus, update_task
from stockquant.util.database import engine
import pandas as pd

_logger = logger.Logger(__name__).get_log()


def update_weekly_task(reset: bool = False):
    """
    更新BS周线后复权任务表
    """
    update_task(table_model=BS_Weekly_hfq, taskEnum=TaskEnum.BS周线历史A股K线后复权数据, reset=reset)


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

    table_name = BS_Weekly_hfq.__tablename__

    try:

        content["date"] = pd.to_datetime(content["date"])
        # content['code'] =
        content["open"] = pd.to_numeric(content["open"], errors="coerce")
        content["high"] = pd.to_numeric(content["high"], errors="coerce")
        content["low"] = pd.to_numeric(content["low"], errors="coerce")
        content["close"] = pd.to_numeric(content["close"], errors="coerce")
        content["volume"] = pd.to_numeric(content["volume"], errors="coerce")
        content["amount"] = pd.to_numeric(content["amount"], errors="coerce")
        # content['adjustflag'] =
        content["turn"] = pd.to_numeric(content["turn"], errors="coerce")
        content["pctChg"] = pd.to_numeric(content["pctChg"], errors="coerce")
        content.to_sql(table_name, engine, if_exists="append", index=False)

    except Exception as e:  # traceback.format_exc(1)
        _logger.error("{}保存出错/{}".format(bs_code, repr(e)))


def get_weekly_hfq():
    """
    按照任务表，获取BS日线后复权行情数据
    """

    query_history_k_data_plus(TaskEnum.BS周线历史A股K线后复权数据, "w", "1", _load_data)
