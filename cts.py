import click
from stockquant.odl.tushare import ts_trade_cal
from stockquant.odl.tushare import adj_factor


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
@click.option('--reset', type=click.BOOL, default=False, help='是否重置任务列表，默认否')
def update_adj_factor(reset):
    """更新复权因子
    """
    click.confirm("正在更新复权因子，是否继续？", abort=True)

    if reset:
        adj_factor.update_task()

    adj_factor.get_adj_factor()
    click.echo("复权因子更新完成。")


def main():
    cli()


if __name__ == "__main__":
    main()