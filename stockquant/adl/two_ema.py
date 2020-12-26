"""均线——两线法回测
"""
from stockquant.odl.models import BS_Daily
from stockquant.util.models import TaskTable
from stockquant.util.database import session_scope
from stockquant.util.stringhelper import TaskEnum
from stockquant.util.taskmanage import create_bs_task
from tqdm import tqdm
from sqlalchemy import select, String, Boolean
from stockquant.util.database import engine
import pandas as pd


def update_task():
    """
    更新任务列表
    """
    create_bs_task(TaskEnum.BS均线应用_两线法, status=True)


def calc_win_rate():
    """
    计算胜率
    """
    taskEnum = TaskEnum.BS均线应用_两线法
    with session_scope() as sm:
        rp = sm.query(TaskTable).filter(TaskTable.task == taskEnum.value, TaskTable.finished == False).all()  # noqa

        for task in tqdm(rp):
            if task.finished:
                continue

            s = select([BS_Daily.code, BS_Daily.date, BS_Daily.close, BS_Daily.preclose])
            s = s.where(BS_Daily.code == task.bs_code)
            s = s.order_by(BS_Daily.date)
            df = pd.read_sql(s, engine)
            df["复权因子"] = (df["close"] / df["preclose"]).cumprod()
            # df["收盘价_复权"] = (df.iloc[-1]["close"] / df.iloc[-1]["复权因子"]) * df["复权因子"] #前复权
            df["收盘价_复权"] = (df.iloc[0]["close"] / df.iloc[0]["复权因子"]) * df["复权因子"]  # 后复权
            df["涨跌幅"] = (df["close"] - df["preclose"]) * 100 / df["preclose"]

            df["EMA_short"] = df["收盘价_复权"].ewm(span=5, adjust=False).mean()
            df["EMA_long"] = df["收盘价_复权"].ewm(span=10, adjust=False).mean()
            df["DIF"] = df["EMA_short"] - df["EMA_long"]

            # 短期均线上穿长期均线-金叉，产生买入信号
            condition1 = df["DIF"] > 0
            condition2 = df["DIF"].shift(1) <= 0
            df.loc[condition1 & condition2, "EMA_signal"] = 1

            # 短期均线上穿长期均线-死叉，产生买入信号
            condition1 = df["DIF"] < 0
            condition2 = df["DIF"].shift(1) >= 0
            df.loc[condition1 & condition2, "EMA_signal"] = 0

            # 计算N日后涨跌幅
            day_list = [1, 5, 20]
            for i in day_list:
                df[f"{i}日后涨跌幅"] = (df["收盘价_复权"].shift(-i) - df["收盘价_复权"]) / df["收盘价_复权"]
                df[f"{i}日后是否上涨"] = df[f"{i}日后涨跌幅"] > 0
                df[f"{i}日后是否上涨"].fillna(value=False, inplace=True)

            股票代码 = []
            信号列表 = []
            N日涨跌幅X概率列表 = []
            概率列表 = []

            # 统计数据
            for signal, group in df.groupby("EMA_signal"):
                for i in day_list:
                    if signal == 1:
                        股票代码.append(task.bs_code)
                        信号列表.append(1)
                        N日涨跌幅X概率列表.append(f"{i}天后涨跌幅大于0概率")
                        概率列表.append(float(group[group[f"{i}日后涨跌幅"] > 0].shape[0]) / group.shape[0])

                    elif signal == 0:
                        股票代码.append(task.bs_code)
                        信号列表.append(0)
                        N日涨跌幅X概率列表.append(f"{i}天后涨跌幅小于0概率")
                        概率列表.append(float(group[group[f"{i}日后涨跌幅"] < 0].shape[0]) / group.shape[0])

            r = pd.DataFrame({"股票代码": 股票代码, "信号": 信号列表, "N日涨跌幅X概率": N日涨跌幅X概率列表, "概率": 概率列表})

            dtype = {"股票代码": String(10), "信号": Boolean(), "N日涨跌幅X概率": String(15)}

            r.to_sql("tmp_two_ema1", engine, if_exists="append", index=False, dtype=dtype)
