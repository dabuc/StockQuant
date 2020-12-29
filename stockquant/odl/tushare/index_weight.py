# -*- coding: utf-8 -*-
"""
指数成分和权重
"""
from stockquant.settings import CQ_Config
from stockquant.util.stringhelper import TaskEnum
from stockquant.odl.models import TS_Index_Daily, TS_Index_Weight
from stockquant.util.database import get_new_session, session_scope, engine
from stockquant.util.models import TaskTable
from datetime import datetime
import calendar
from stockquant.util import logger
from tqdm import tqdm
import tushare as ts
import time
import pandas as pd
from sqlalchemy import distinct

_logger = logger.Logger(__name__).get_log()
task = TaskEnum.TS指数成分和权重


def update_task():
    """
    更新任务表
    """
    global task

    TaskTable.del_with_task(task)

    with session_scope() as sm:
        query = sm.query(distinct(TS_Index_Daily.ts_code).label("ts_code"))
        codes = query.all()

        now = datetime.now()
        # this_month_start = datetime(now.year, now.month, 1).date()
        this_month_start = datetime(2018, 1, 1).date()
        this_month_end = datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=task.value,
                task_name=task.name,
                ts_code=c.ts_code,
                bs_code="NA",
                begin_date=this_month_start,
                end_date=this_month_end,
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


def _load_data(content: pd.DataFrame, ts_code):
    """
    做一些简单转换后，加载数据到数据库
    """
    table_name = TS_Index_Weight.__tablename__

    if content.empty:
        return

    try:
        content["trade_date"] = pd.to_datetime(content["trade_date"])
        content.to_sql(table_name, engine, if_exists="append", index=False)
    except Exception as e:
        _logger.error(f"{ts_code}-日线行情保存出错/{repr(e)}")


def get_index_weight():
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
                    begin_date = datetime.strftime(task.begin_date, "%Y%m%d")
                    end_date = datetime.strftime(task.end_date, "%Y%m%d")
                    df = pro.index_weight(index_code=task.ts_code, start_date=begin_date, end_date=end_date)
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
                        _logger.error(f"获取指数成分和权重失败/获取[{task.ts_code}]指数成分和权重失败：{repr(e)}")
                        raise
        sm.commit()
    except:  # noqa
        sm.commit()
        raise
    finally:
        sm.close()
