# -*- coding: utf-8 -*-
"""数据仓库管理工具"""
from stockquant.odl.tushare import stock_basic, ts_trade_cal, daily_basic, daily
from stockquant.odl.baostock import bs_daily, bs_stock_basic
import click
from stockquant.util.database import engine
from stockquant.odl import models as odl_model
from stockquant.util import models as orm_model


@click.group()
def cli():
    """操作数据层"""
    pass


@cli.command()
def create_dw():
    """创建数据仓库"""
    click.confirm("正在创建操作数据层数据表，是否继续？", abort=True)

    odl_model.Base.metadata.create_all(engine)
    # bdl_model.Base.metadata.create_all(engine)
    orm_model.Base.metadata.create_all(engine)
    # dim_model.Base.metadata.create_all(engine)

    click.echo("数据层数据表创建完成")


@cli.command()
def update_stock_info():
    """
    更新股票信息：股票列表、交易日历、BS日线K线行情、TS每日指标
    """
    click.confirm("正在更新股票信息，是否继续？", abort=True)
    bs_stock_basic.get_stock_basic()
    click.echo("bs-A股股票列表更新完成。")
    stock_basic.get_stock_basic()
    click.echo("TS股票基础信息更新完成。")
    ts_trade_cal.create_cal_date()
    click.echo("交易日历更新完成")

    daily_basic.update_task()
    daily_basic.update_daily_basic()
    click.echo("TS每日指标更新完成。")

    daily.update_task()
    daily.get_daily()
    click.echo("TS日线行情更新完成。")

    bs_daily.update_task()
    bs_daily.get_daily()
    click.echo("BS日线行情数据更新完成。")


def main():
    cli()


if __name__ == "__main__":
    main()
