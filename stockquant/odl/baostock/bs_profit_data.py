import concurrent.futures
import time
from datetime import datetime, timedelta
import pandas as pd
from stockquant.settings import CQ_Config
from stockquant.util import logger
from stockquant.util.database import engine, session_scope
from stockquant.util.models import TaskTable
from stockquant.util.stringhelper import TaskEnum
from stockquant.util.taskmanage import create_task

import baostock as bs

_logger = logger.Logger(__name__).get_log()


def create_profit_data_task():
    """
    创建季频盈利能力数据任务
    """
    begin_date = datetime.strptime("2007-01-01", "%Y-%m-%d").date()
    end_date = datetime.now().date() + timedelta(365)
    create_task(TaskEnum.季频盈利能力, begin_date, end_date, type="1", isdel=True)  # 股票


def _save_date(dflist: list, code: str):
    """
    保存数据
    """
    result_profit = pd.concat(dflist)
    result_profit.to_sql("odl_bs_profit_data", engine, schema=CQ_Config.DB_SCHEMA, if_exists="append", index=False)
    _logger.info("{}: 保存数据成功".format(code))


def get_profit_data():
    """
    获取季频盈利能力数据
    """
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print("login respond error_code:" + lg.error_code)
    print("login respond  error_msg:" + lg.error_msg)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        with session_scope() as sm:
            rp = sm.query(TaskTable).filter(TaskTable.task == TaskEnum.季频盈利能力.value, TaskTable.finished == False)

            for task in rp:
                if task.finished:
                    continue

                start_year = task.begin_date.year
                end_year = task.end_date.year

                dflist = []
                for y in range(start_year, end_year):
                    for q in range(1, 5):

                        max_try = 8  # 失败重连的最大次数
                        for i in range(max_try):
                            rs_profit = bs.query_profit_data(code=task.ts_code, year=y, quarter=q)
                            if rs_profit.error_code == "0":
                                profit_list = []
                                while (rs_profit.error_code == "0") & rs_profit.next():
                                    profit_list.append(rs_profit.get_row_data())
                                result_df = pd.DataFrame(profit_list, columns=rs_profit.fields)
                                dflist.append(result_df)

                                break
                            elif i < (max_try - 1):
                                time.sleep(2)
                                _logger.error("{}:{}-{} 获取数据失败，重连……".format(task.ts_code, y, q))
                                continue

                task.finished = True
                executor.submit(_save_date, dflist, task.ts_code)
        sm.commit()

    # 登出系统
    bs.logout()


if __name__ == "__main__":
    pass
