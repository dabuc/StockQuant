# -*- coding: utf-8 -*-
"""
指数日线行情
"""
from stockquant.settings import CQ_Config
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.models import TS_Index_Basic, TS_Index_Daily
from stockquant.util.database import get_new_session, session_scope, engine
from stockquant.util.models import TaskTable
from datetime import datetime
from stockquant.util import logger
from tqdm import tqdm
import tushare as ts
import time
import pandas as pd

_logger = logger.Logger(__name__).get_log()
task = TaskEnum.TS指数日线行情


def update_task():
    """
    更新任务表
    """
    global task

    TaskTable.del_with_task(task)

    with session_scope() as sm:
        query = sm.query(TS_Index_Basic.ts_code, TS_Index_Basic.list_date)
        query = query.filter(TS_Index_Basic.market != "SW")
        codes = query.all()

        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=task.value,
                task_name=task.name,
                ts_code=c.ts_code,
                bs_code="NA",
                begin_date=c.list_date if c.list_date else datetime(1990, 12, 19).date(),
                end_date=datetime.now().date(),
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


def _load_data(content: pd.DataFrame, ts_code):
    """
    做一些简单转换后，加载数据到数据库
    """
    table_name = TS_Index_Daily.__tablename__

    if content.empty:
        return

    try:
        content["trade_date"] = pd.to_datetime(content["trade_date"])
        content.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        _logger.error(f"{ts_code}-日线行情保存出错/{repr(e)}")


def get_index_daily():
    """
    更新TS指数日线行情
    """
    global task
    pro = ts.pro_api(CQ_Config.TUSHARE_TOKEN)
    sm = get_new_session()
    try:
        rp = sm.query(TaskTable).filter(TaskTable.task == task.value, TaskTable.finished == False)  # noqa
        if CQ_Config.IDB_DEBUG == "1":  # 如果是测试环境
            rp = rp.limit(1000)

        rp = rp.all()

        for task in tqdm(rp):
            if task.finished:
                continue

            max_try = 8  # 失败重连的最大次数
            for i in range(max_try):
                try:
                    df = pro.index_daily(ts_code=task.ts_code)
                    _load_data(content=df, ts_code=task.ts_code)
                    task.finished = True
                    break
                except Exception as e:
                    if i < (max_try - 1):
                        t = (i + 1) * 2
                        time.sleep(t)
                        _logger.warning(f"[{task.ts_code}]异常重连/{repr(e)}")
                        continue
                    else:
                        _logger.error(f"获取指数日线行情失败/获取[{task.ts_code}]日线行情失败：{repr(e)}")
                        raise
        sm.commit()
    except:  # noqa
        sm.commit()
        raise
    finally:
        sm.close()
