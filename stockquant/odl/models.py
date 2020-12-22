from sqlalchemy import (
    UniqueConstraint,
    Column,
    Integer,
    BigInteger,
    Numeric,
    String,
    Enum,
    Float,
    Boolean,
    Date,
    DateTime,
)
from datetime import datetime as dtime
from stockquant.util.database import Base


class BS_Stock_Basic(Base):
    """
    BS-证券基本资料
    """

    __tablename__ = "odl_bs_stock_basic"
    code = Column(String(10), primary_key=True)  # 证券代码
    code_name = Column(String(100))  # 证券名称
    ipoDate = Column(Date)  # 上市日期
    outDate = Column(Date)  # 退市日期
    type = Column(Enum("1", "2", "3"))  # 证券类型，其中1：股票，2：指数,3：其它
    status = Column(Boolean)  # 上市状态，其中1：上市，0：退市
    ts_code = Column(String(10))  # ts_证券代码
    updated_on = Column(DateTime, default=dtime.now, onupdate=dtime.now)


class BS_Daily_Base:
    """
    BS日线历史行情数据基类
    """

    id = Column("id", Integer, primary_key=True)
    date = Column("date", Date, nullable=False)  # 交易所行情日期
    code = Column("code", String(10), nullable=False)  # BS证券代码 格式：sh.600000。sh：上海，sz：深圳
    open = Column("open", Numeric(18, 4), nullable=False)  # 今开盘价格 精度：小数点后4位；单位：人民币元
    high = Column("high", Numeric(18, 4), nullable=False)  # 最高价 精度：小数点后4位；单位：人民币元
    low = Column("low", Numeric(18, 4), nullable=False)  # 最低价 精度：小数点后4位；单位：人民币元
    close = Column("close", Numeric(18, 4), nullable=False)  # 今收盘价 精度：小数点后4位；单位：人民币元
    preclose = Column("preclose", Numeric(18, 4))  # 昨日收盘价 精度：小数点后4位；单位：人民币元
    volume = Column("volume", BigInteger)  # 成交数量 单位：股
    amount = Column("amount", Numeric(23, 4))  # 成交金额	精度：小数点后4位；单位：人民币元
    adjustflag = Column("adjustflag", Enum("1", "2", "3"))  # 复权状态(1：后复权， 2：前复权，3：不复权）
    turn = Column("turn", Numeric(18, 6))  # 换手率 精度：小数点后6位；单位：%
    tradestatus = Column("tradestatus", Boolean)  # 交易状态	1：正常交易 0：停牌
    pctChg = Column("pctChg", Numeric(18, 6))  # 涨跌幅（百分比）	精度：小数点后6位
    peTTM = Column("peTTM", Numeric(18, 6))  # 滚动市盈率	精度：小数点后6位
    psTTM = Column("psTTM", Numeric(18, 6))  # 滚动市销率	精度：小数点后6位
    pcfNcfTTM = Column("pcfNcfTTM", Numeric(18, 6))  # 滚动市现率	精度：小数点后6位
    pbMRQ = Column("pbMRQ", Numeric(18, 6))  # 市净率	精度：小数点后6位
    isST = Column("isST", Boolean)  # 是否ST	1是，0否
    __table_args__ = (UniqueConstraint("code", "date", name="UDX_CODE_DATE"),)


def default_t_date(context):

    datestr = context.get_current_parameters()["date"]
    t_date = dtime.strptime(datestr, "%Y-%m-%d").date()
    return t_date


class BS_Weekly_Base:
    """
    周线历史行情数据
    """

    id = Column("id", Integer, primary_key=True)
    date = Column("date", String(10))
    code = Column("code", String(10))
    open = Column("open", String(15))
    high = Column("high", String(15))
    low = Column("low", String(15))
    close = Column("close", String(15))
    volume = Column("volume", String(20))
    amount = Column("amount", String(23))
    adjustflag = Column("adjustflag", String(1))
    turn = Column("turn", String(15))
    pctChg = Column("pctChg", String(15))
    t_date = Column("t_date", Date, default=default_t_date)


class BS_Monthly_Base:
    """
    月线历史行情数据
    """

    id = Column("id", Integer, primary_key=True)
    date = Column("date", String(10))
    code = Column("code", String(10))
    open = Column("open", String(15))
    high = Column("high", String(15))
    low = Column("low", String(15))
    close = Column("close", String(15))
    volume = Column("volume", String(20))
    amount = Column("amount", String(23))
    adjustflag = Column("adjustflag", String(1))
    turn = Column("turn", String(15))
    pctChg = Column("pctChg", String(15))
    t_date = Column("t_date", Date, default=default_t_date)


class BS_15m_Base:
    """
    5分钟线历史行情数据
    """

    id = Column("id", BigInteger, primary_key=True)
    date = Column("date", String(10))
    time = Column("time", String(10))
    code = Column("code", String(10))
    open = Column("open", String(15))
    high = Column("high", String(15))
    low = Column("low", String(15))
    close = Column("close", String(15))
    volume = Column("volume", String(20))
    amount = Column("amount", String(23))
    adjustflag = Column("adjustflag", String(1))


# ------后复权------------
class BS_Daily_hfq(BS_Daily_Base, Base):
    """
    后复权-日线历史行情数据
    """

    __tablename__ = "odl_bs_daily_hfq"


class BS_SZ50_Stocks(Base):
    """
    上证50
    """

    __tablename__ = "odl_bs_sz50_stocks"
    id = Column("id", Integer, primary_key=True)
    updateDate = Column("updateDate", Date)
    code = Column("code", String(10))
    code_name = Column("code_name", String(10))


# =========================Tushare数据源模型============================
class TS_TradeCal(Base):
    """
    交易日历
    """

    __tablename__ = "odl_ts_trade_cal"
    id = Column("id", Integer, primary_key=True)
    exchange = Column("exchange", String(10), nullable=False)  # 交易所 SSE上交所 SZSE深交所
    cal_date = Column("cal_date", Integer, nullable=False)  # 日历日期
    date = Column("date", Date, nullable=False)  # 日历日期
    is_open = Column("is_open", Boolean, nullable=False)  # 是否交易 0休市 1交易
    pretrade_date = Column("pretrade_date", Date)  # 上一个交易日


class TS_Stock_Basic(Base):
    """
    TS-证券基本资料
    """

    __tablename__ = "odl_ts_stock_basic"
    ts_code = Column("ts_code", String(10), primary_key=True)  # TS代码
    symbol = Column("symbol", String(6))  # 股票代码
    name = Column("name", String(10))  # 股票名称
    area = Column("area", String(4))  # 所在地域
    industry = Column("industry", String(4))  # 所属行业
    fullname = Column("fullname", String(25))  # 股票全称
    enname = Column("enname", String(100))  # 英文全称
    market = Column("market", String(3))  # 市场类型 （主板/中小板/创业板/科创板）
    exchange = Column("exchange", String(4), nullable=False)  # 交易所代码
    curr_type = Column("curr_type", String(3))  # 交易货币
    list_status = Column("list_status", String(1), nullable=False)  # 上市状态： L上市 D退市 P暂停上市
    list_date = Column("list_date", Date)  # 上市日期
    delist_date = Column("delist_date", Date)  # 退市日期
    is_hs = Column("is_hs", String(1))  # 是否沪深港通标的，N否 H沪股通 S深股通
    bs_code = Column("bs_code", String(10), index=True)  # BS代码


class TS_Daily_Basic(Base):
    """
    每日指标
    """

    __tablename__ = "odl_ts_daily_basic"
    id = Column("id", Integer, primary_key=True)
    ts_code = Column("ts_code", String(10), nullable=False)  # TS股票代码
    trade_date = Column("trade_date", Date, nullable=False)  # 交易日期
    close = Column("close", Numeric(18, 5))  # 当日收盘价 7,4
    turnover_rate = Column("turnover_rate", Numeric(18, 5))  # 换手率（%） 8,4
    turnover_rate_f = Column("turnover_rate_f", Numeric(18, 5))  # 换手率（自由流通股）9,4
    volume_ratio = Column("volume_ratio", Numeric(18, 3))  # 量比 8,2
    pe = Column("pe", Numeric(18, 5))  # 市盈率（总市值/净利润， 亏损的PE为空）10,4
    pe_ttm = Column("pe_ttm", Numeric(18, 5))  # 市盈率（TTM，亏损的PE为空）12,4
    pb = Column("pb", Numeric(18, 5))  # 市净率（总市值/净资产）10,4
    ps = Column("ps", Numeric(18, 5))  # 市销率 11,4
    ps_ttm = Column("ps_ttm", Numeric(18, 5))  # 市销率（TTM）15,4
    dv_ratio = Column("dv_ratio", Numeric(18, 5))  # 股息率 （%）7,4
    dv_ttm = Column("dv_ttm", Numeric(18, 5))  # 股息率（TTM）（%）7,4
    total_share = Column("total_share", Numeric(18, 5))  # 总股本 （万股）13,4
    float_share = Column("float_share", Numeric(18, 5))  # 流通股本 （万股）13,4
    free_share = Column("free_share", Numeric(18, 5))  # 自由流通股本 （万）12,4
    total_mv = Column("total_mv", Numeric(18, 5))  # 总市值 （万元）14,4
    circ_mv = Column("circ_mv", Numeric(18, 5))  # 流通市值（万元）14,4
    bs_code = Column("bs_code", String(10), nullable=False)  # TS股票代码


class TS_Adj_Factor(Base):
    """
    复权因子
    """

    __tablename__ = "odl_ts_adj_factor"
    id = Column("id", Integer, primary_key=True)
    ts_code = Column("ts_code", String(10), nullable=False)
    trade_date = Column("trade_date", Date, nullable=False)
    adj_factor = Column("adj_factor", Float, nullable=False)


class TS_Daily_Base:
    """
    日线行情数据
    """

    id = Column("id", Integer, primary_key=True)
    ts_code = Column("ts_code", String(10), nullable=False)  # 股票代码
    trade_date = Column("trade_date", Date, nullable=False)  # 交易日期
    open = Column("open", Numeric(12, 4), nullable=False)  # 开盘价
    high = Column("high", Numeric(12, 4), nullable=False)  # 最高价
    low = Column("low", Numeric(12, 4), nullable=False)  # 最低价
    close = Column("close", Numeric(12, 4), nullable=False)  # 收盘价
    pre_close = Column("pre_close", Numeric(12, 4), nullable=False)  # 昨收价
    change = Column("change", Numeric(12, 4))  # 涨跌额
    pct_chg = Column("pct_chg", Numeric(12, 4))  # 涨跌幅
    vol = Column("vol", Numeric(23, 4))  # 成交量 （手）
    amount = Column("amount", Numeric(23, 4))  # 成交额 （千元）
    __table_args__ = (UniqueConstraint("ts_code", "trade_date", name="UDX_CODE_DATE"),)


class TS_Daily(TS_Daily_Base, Base):
    """
    日线行情数据
    """

    __tablename__ = "odl_ts_daily"


# class TS_Daily_Test(Base):
#     """
#     日线行情数据
#     """

#     __tablename__ = "测试表"

#     id = Column("id", Integer, primary_key=True)
#     股票代码 = Column("股票代码", String(10), nullable=False)  # 股票代码
#     交易日期 = Column("交易日期", Date, nullable=False)  # 交易日期
