from stockquant.odl.models import TS_Daily
from sqlalchemy import select
from stockquant.util.database import engine
import pandas as pd

s = (
    select([TS_Daily.ts_code, TS_Daily.trade_date, TS_Daily.close, TS_Daily.pre_close])
    .where(TS_Daily.ts_code == "600036.SH")
    .order_by(TS_Daily.trade_date)
)

df = pd.read_sql(s, engine)
print(df.dtypes)
df["复权因子"] = (df["close"] / df["pre_close"]).cumprod()
df["收盘价_前复权"] = (df.iloc[-1]["close"] / df.iloc[-1]["复权因子"]) * df["复权因子"]
df["收盘价_后复权"] = (df.iloc[0]["close"] / df.iloc[0]["复权因子"]) * df["复权因子"]
df["涨跌幅"] = (df["close"] - df["pre_close"]) * 100 / df["pre_close"]

df["EMA_short"] = df["收盘价_后复权"].ewm(span=12, adjust=False).mean()
df["EMA_long"] = df["收盘价_后复权"].ewm(span=26, adjust=False).mean()
df["DIF"] = df["EMA_short"] - df["EMA_long"]
df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
df["MACD"] = (df["DIF"] - df["DEA"]) * 2

# macd转正，产生买入信号
condition1 = df["MACD"] > 0
condition2 = df["MACD"].shift(1) <= 0
df.loc[condition1 & condition2, "macd_signal"] = 1

# macd转负，产生卖出信号
condition1 = df["MACD"] < 0
condition2 = df["MACD"].shift(1) >= 0
df.loc[condition1 & condition2, "macd_signal"] = 0


# 计算N日后涨跌幅
day_list = [1, 5, 20]
for i in day_list:
    df["%s日后涨跌幅" % i] = (df["收盘价_后复权"].shift(-i) - df["收盘价_后复权"]) / df["收盘价_后复权"]
    df["%s日后是否上涨" % i] = df["%s日后涨跌幅" % i] > 0
    df["%s日后是否上涨" % i].fillna(value=False, inplace=True)

# 统计数据
for signal, group in df.groupby("macd_signal"):
    print(signal)
    print(group[[str(i) + "日后涨跌幅" for i in day_list]].describe())
    for i in day_list:
        if signal == 1:
            print(str(i) + "天后涨跌幅大于0概率", "\t", float(group[group[str(i) + "日后涨跌幅"] > 0].shape[0]) / group.shape[0])
        elif signal == 0:
            print(str(i) + "天后涨跌幅小于0概率", "\t", float(group[group[str(i) + "日后涨跌幅"] < 0].shape[0]) / group.shape[0])


# print(df)
df.to_csv("600036.csv", index=False)

if __name__ == "__main__":
    pass
