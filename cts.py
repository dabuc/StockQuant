import click
from stockquant.odl.tushare import daily, ts_trade_cal, daily_basic, adj_factor, stk_limit, stock_basic


@click.group()
def cli():
    """命令集"""
    pass


@cli.command()
def create_cal_date():
    """
    创建交易日历
    """
    ts_trade_cal.create_cal_date()
    click.echo("创建交易日历完成")


@cli.command()
@click.option("--reset", type=click.BOOL, default=False, help="是否重置任务列表，默认否")
def update_adj_factor(reset):
    """更新复权因子"""
    click.confirm("正在更新复权因子，是否继续？", abort=True)

    if reset:
        adj_factor.update_task()

    adj_factor.get_adj_factor()
    click.echo("复权因子更新完成。")


@cli.command()
def update_daily():
    """更新日线行情"""
    click.confirm("正在更新日线行情，是否继续？", abort=True)

    daily.update_task()
    daily.get_daily()
    click.echo("日线行情更新完成。")


@cli.command()
def update_daily_basic():
    """按日期更新每日指标"""
    click.confirm("正在更新每日指标，是否继续？", abort=True)
    daily_basic.update_task()
    daily_basic.update_daily_basic()
    click.echo("每日指标更新完成。")


@cli.command()
def update_stk_limit():
    """按日期更新每日股票涨跌停价"""
    click.confirm("正在更新每日股票涨跌停价，是否继续？", abort=True)
    stk_limit.update_task()
    stk_limit.get_stk_limit()
    click.echo("每日股票涨跌停价更新完成。")


@cli.command()
def update_stock_basic():
    """更新TS股票基础信息"""
    click.confirm("正在更新TS股票基础信息，是否继续？", abort=True)
    stock_basic.get_stock_basic()
    click.echo("TS股票基础信息更新完成。")


def main():
    cli()


if __name__ == "__main__":
    main()
