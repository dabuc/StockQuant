from stockquant.util import logger
from stockquant.util.stringhelper import TaskEnum
from stockquant.util.database import Base, session_scope
from sqlalchemy import Column, Integer, String, Date, Boolean, DateTime
from datetime import datetime as dtime


_logger = logger.Logger(__name__).get_log()


class TaskTable(Base):
    """
    任务配置表
    """

    __tablename__ = "bdl_task_table"
    id = Column(Integer, primary_key=True)
    task = Column(String(30), nullable=False)  # 任务ID
    task_name = Column(String(50), nullable=False)  # 任务
    bs_code = Column(String(10), nullable=False)  # 证券代码 bs格式
    ts_code = Column(String(10), nullable=False)  # 证券代码 ts格式
    begin_date = Column(Date, nullable=False)  # 开始时间
    end_date = Column(Date, nullable=False)  # 结束时间
    finished = Column(Boolean, default=False, nullable=False)  # 是否完成
    remark = Column(String(50))  # 备注信息
    create_on = Column(Date, default=dtime.now().date)
    updated_on = Column(DateTime, default=dtime.now, onupdate=dtime.now, nullable=False)

    @staticmethod
    def del_with_task(taskEnum: TaskEnum):
        """
        按任务ID过滤，删除历史任务列表
        """
        with session_scope() as sm:
            query = sm.query(TaskTable).filter(TaskTable.task == taskEnum.value)
            query.delete()
            sm.commit()
            _logger.info("任务：{}-历史任务已删除".format(taskEnum.name))
