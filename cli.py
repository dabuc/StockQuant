# -*- coding: utf-8 -*-
"""数据仓库管理工具"""
import click
from stockquant.util.database import engine
from stockquant.odl import models as odl_model
from stockquant.util import models as orm_model


@click.group()
def odl():
    """操作数据层"""
    pass


@odl.command()
def create_dw():
    """创建数据仓库"""
    click.confirm("正在创建操作数据层数据表，是否继续？", abort=True)

    odl_model.Base.metadata.create_all(engine)
    # bdl_model.Base.metadata.create_all(engine)
    orm_model.Base.metadata.create_all(engine)
    # dim_model.Base.metadata.create_all(engine)

    click.echo("数据层数据表创建完成")


def main():
    odl()


if __name__ == "__main__":
    main()
