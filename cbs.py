import click
from stockquant.odl.baostock import bs_daily, bs_daily_hfq, bs_sz50_stocks, bs_weekly_hfq
from stockquant.odl.baostock import bs_stock_basic

# from coralquant.bdl.baostock import ln_pctChg


@click.group()
def cli():
    """命令集"""
    pass


@cli.command()
def update_bs_stock_basic():
    """更新bs-A股股票列表"""
    bs_stock_basic.get_stock_basic()
    click.echo("bs-A股股票列表更新完成。")


@cli.command()
def get_sz50_stocks():
    """
    获取上证50成分股
    """

    bs_sz50_stocks.get_sz50_stocks()
    click.echo("获取上证50成分股完成")


@cli.command()
@click.option("--reset", type=click.BOOL, default=False, help="是否重置任务列表，默认否")
def update_daily_hfq(reset):
    """更新日线行情后复权数据"""
    click.confirm("正在更新BS日线后复权行情数据，是否继续？", abort=True)

    bs_daily_hfq.update_task(reset)

    bs_daily_hfq.get_daily_hfq()

    click.echo("BS日线后复权行情数据更新完成。")


@cli.command()
def update_daily():
    """更新日线行情数据"""
    click.confirm("正在更新BS日线行情数据，是否继续？", abort=True)

    bs_daily.update_task()

    bs_daily.get_daily()

    click.echo("BS日线行情数据更新完成。")


@cli.command()
@click.option("--reset", type=click.BOOL, default=False, help="是否重置任务列表，默认否")
def update_weekly_hfq(reset):
    """更新BS周线后复权数据"""
    click.confirm("正在更新BS更新周线后复权数据，是否继续？", abort=True)

    bs_weekly_hfq.update_weekly_task(reset)
    bs_weekly_hfq.get_weekly_hfq()

    click.echo("BS周线后复权数据更新完成。")


# @cli.command()
# def update_ln_pctChg():
#     """更新BS计算指定日期往后N日涨跌幅"""
#     click.confirm("正在更新BS计算指定日期往后N日涨跌幅，是否继续？", abort=True)

#     ln_pctChg.calc_later_n_pctChg()

#     click.echo("BS计算指定日期往后N日涨跌幅更新完成。")


def main():
    cli()


if __name__ == "__main__":
    main()