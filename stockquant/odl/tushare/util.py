from stockquant.util import logger
from datetime import datetime
from stockquant.settings import CQ_Config
from stockquant.util.models import TaskTable
from stockquant.util.stringhelper import TaskEnum
import concurrent.futures
from stockquant.util.database import get_new_session
from tqdm import tqdm
import time
import copy
from sqlalchemy import desc

_logger = logger.Logger(__name__).get_log()


def extract_data(
    taskEnum: TaskEnum,
    pro_api_func,
    pro_api_func_pramas: dict,
    load_data_func,
    load_data_func_params: dict,
    log_desc: str,
):
    """
    抽取远端数据
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        sm = get_new_session()
        try:
            rp = sm.query(TaskTable).filter(TaskTable.task == taskEnum.value, TaskTable.finished == False)  # noqa
            if CQ_Config.IDB_DEBUG == "1":  # 如果是测试环境
                rp = rp.order_by(desc(TaskTable.begin_date))
                rp = rp.limit(30)

            rp = rp.all()

            for task in tqdm(rp):
                if task.finished:
                    continue

                max_try = 8  # 失败重连的最大次数
                for i in range(max_try):
                    try:
                        tasktime = datetime.strftime(task.begin_date, "%Y%m%d")
                        if pro_api_func_pramas:
                            pramas = copy.deepcopy(pro_api_func_pramas)
                        else:
                            pramas = {}
                        pramas["trade_date"] = tasktime
                        result = pro_api_func(**pramas)

                        if load_data_func_params:
                            load_pramas = copy.deepcopy(load_data_func_params)
                        else:
                            load_pramas = {}

                        load_pramas["result"] = result
                        load_pramas["task_date"] = task.begin_date
                        executor.submit(load_data_func, load_pramas)

                        task.finished = True
                        time.sleep(0.2)
                        break
                    except Exception as e:
                        if i < (max_try - 1):
                            t = (i + 1) * 2
                            time.sleep(t)
                            logger.error("[{}]异常重连/{}".format(task.ts_code, repr(e)))
                            continue
                        else:
                            _logger.error("获取[{}]{}失败/{}".format(task.ts_code, log_desc, repr(e)))
                            raise
            sm.commit()
        except:  # noqa
            sm.commit()
            raise
        finally:
            sm.close()
