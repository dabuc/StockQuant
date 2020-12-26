from stockquant.odl.models import BS_Stock_Basic
from stockquant.util.taskmanage import create_bs_task
from stockquant.util.stringhelper import TaskEnum
from stockquant.util import logger
from stockquant.util.models import TaskTable
from stockquant.util.database import session_scope
import baostock as bs
import concurrent.futures
from tqdm import tqdm
import pandas as pd
import time
import copy
from sqlalchemy import func, or_, and_
from datetime import datetime, timedelta

_logger = logger.Logger(__name__).get_log()


def _get_fields(frequency="d") -> str:
    """
    获取历史A股K线数据的字段列
    """
    d_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,\
    tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    w_m_fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
    min_fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"

    frequency_fields = {
        "d": d_fields,
        "w": w_m_fields,
        "m": w_m_fields,
        "5": min_fields,
        "15": min_fields,
        "30": min_fields,
        "60": min_fields,
    }

    return frequency_fields[frequency]


def query_history_k_data_plus(
    taskEnum,
    frequency,
    adjustflag,
    load_data_func,
    load_data_func_params: dict = None,
):
    """
    按照任务表获取历史A股K线数据
    """

    fields = _get_fields(frequency)

    # ### 登陆系统 ####
    lg = bs.login()  # noqa

    with concurrent.futures.ThreadPoolExecutor() as executor:
        with session_scope() as sm:
            rp = (
                sm.query(TaskTable).filter(TaskTable.task == taskEnum.value, TaskTable.finished == False).all()  # noqa
            )
            for task in tqdm(rp):
                if task.finished:
                    continue

                start_date = task.begin_date.strftime("%Y-%m-%d")
                end_date = task.end_date.strftime("%Y-%m-%d")

                max_try = 8  # 失败重连的最大次数

                for i in range(max_try):
                    rs = bs.query_history_k_data_plus(
                        task.bs_code,
                        fields,
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        adjustflag=adjustflag,
                    )
                    if rs.error_code == "0":
                        data_list = []
                        while (rs.error_code == "0") & rs.next():
                            # 获取一条记录，将记录合并在一起
                            data_list.append(rs.get_row_data())
                        # _logger.info('{}下载成功,数据{}条'.format(task.ts_code, len(data_list)))
                        result = pd.DataFrame(data_list, columns=rs.fields)

                        if load_data_func_params:
                            params = copy.deepcopy(load_data_func_params)
                        else:
                            params = {}
                        params["result"] = result
                        params["bs_code"] = task.bs_code
                        params["frequency"] = frequency
                        params["adjustflag"] = adjustflag
                        executor.submit(load_data_func, params)
                        task.finished = True
                        break
                    elif i < (max_try - 1):
                        time.sleep(2)
                        continue
                    else:
                        _logger.error("获取历史A股K线数据失败/query_history_k_data_plus respond error_code:" + rs.error_code)
                        _logger.error("获取历史A股K线数据失败/query_history_k_data_plus respond  error_msg:" + rs.error_msg)
            sm.commit()
    # ### 登出系统 ####
    bs.logout()


def update_task(table_model, taskEnum: TaskEnum, reset: bool = False):
    """
    更新任务表：日线、周线
    """
    if reset:
        create_bs_task(taskEnum)
        return

    # 删除历史任务记录
    TaskTable.del_with_task(taskEnum)

    with session_scope() as sm:
        # 通过BS证券基本资料和A股K线后复权数据的每个股票的最新交易时间，查出所有需要更新的股票及更新时间
        cte = (
            sm.query(table_model.code, func.max(table_model.date).label("mx_date"))
            .group_by(table_model.code)
            .cte("cte")
        )
        query = sm.query(
            BS_Stock_Basic.code, BS_Stock_Basic.ts_code, BS_Stock_Basic.ipoDate, BS_Stock_Basic.outDate, cte.c.mx_date
        )
        query = query.join(cte, BS_Stock_Basic.code == cte.c.code, isouter=True)
        query = query.filter(
            or_(
                and_(BS_Stock_Basic.outDate == None, cte.c.mx_date < datetime.now().date()),  # noqa
                cte.c.mx_date == None,
                BS_Stock_Basic.outDate > cte.c.mx_date,
            )
        )

        codes = query.all()
        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=taskEnum.value,
                task_name=taskEnum.name,
                ts_code=c.ts_code,
                bs_code=c.code,
                begin_date=c.ipoDate if c.mx_date is None else c.mx_date + timedelta(days=1),
                end_date=c.outDate if c.outDate is not None else datetime.now().date(),
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))
