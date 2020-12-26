# -*- coding: utf-8 -*-
from stockquant.odl.models import BS_Stock_Basic, BS_SZ50_Stocks, TS_Stock_Basic, TS_TradeCal
from stockquant.util import logger
from datetime import datetime
from stockquant.util.database import session_scope
from stockquant.settings import CQ_Config
from stockquant.util.models import TaskTable
from stockquant.util.stringhelper import TaskEnum
from sqlalchemy import distinct

_logger = logger.Logger(__name__).get_log()


def create_bs_task(task: TaskEnum, tmpcodes=None, status: bool = None):
    """
    创建BS任务列表
    """
    # 删除原有的相同任务的历史任务列表
    TaskTable.del_with_task(task)

    with session_scope() as sm:
        query = sm.query(BS_Stock_Basic.code, BS_Stock_Basic.ipoDate, BS_Stock_Basic.outDate, BS_Stock_Basic.ts_code)
        if CQ_Config.IDB_DEBUG == "1":  # 如果是测试环境
            if tmpcodes:
                query = query.filter(BS_Stock_Basic.code.in_(tmpcodes))
            else:
                query = query.join(BS_SZ50_Stocks, BS_Stock_Basic.code == BS_SZ50_Stocks.code)
        if status:
            query = query.filter(BS_Stock_Basic.status == status)

        codes = query.all()

        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=task.value,
                task_name=task.name,
                ts_code=c.ts_code,
                bs_code=c.code,
                begin_date=c.ipoDate,
                end_date=c.outDate if c.outDate is not None else datetime.now().date(),
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


def create_ts_task(task: TaskEnum):
    """
    创建TS任务列表
    """
    # 删除原有的相同任务的历史任务列表
    TaskTable.del_with_task(task)

    with session_scope() as sm:

        codes = (
            sm.query(
                TS_Stock_Basic.ts_code, TS_Stock_Basic.bs_code, TS_Stock_Basic.list_date, TS_Stock_Basic.delist_date
            )
            .filter(TS_Stock_Basic.list_status == "L")
            .all()
        )

        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=task.value,
                task_name=task.name,
                ts_code=c.ts_code,
                bs_code=c.bs_code,
                begin_date=c.list_date,
                end_date=c.delist_date if c.delist_date is not None else datetime.now().date(),
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


def create_ts_cal_task(task: TaskEnum):
    """
    创建基于交易日历的任务列表
    """
    # 删除历史任务
    TaskTable.del_with_task(task)

    with session_scope() as sm:
        rp = sm.query(distinct(TS_TradeCal.date).label("t_date")).filter(
            TS_TradeCal.is_open == True, TS_TradeCal.date <= datetime.now().date()  # noqa
        )
        codes = rp.all()
        tasklist = []
        for c in codes:
            tasktable = TaskTable(
                task=task.value,
                task_name=task.name,
                ts_code="按日期更新",
                bs_code="按日期更新",
                begin_date=c.t_date,
                end_date=c.t_date,
            )
            tasklist.append(tasktable)
        sm.bulk_save_objects(tasklist)
    _logger.info("生成{}条任务记录".format(len(codes)))


if __name__ == "__main__":
    pass
