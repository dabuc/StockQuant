from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockquant.settings import CQ_Config

engine = create_engine(CQ_Config.DATABASE_URL)

session_factory = sessionmaker(bind=engine)


class Base(object):
    """StockQuant的Base基类"""

    @classmethod
    def clear_table(cls):
        """
        清空数据表
        """
        with session_scope() as sn:
            sn.query(cls).delete()


Base = declarative_base(cls=Base)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = session_factory()
    try:
        yield session
        session.commit()
    except:  # noqa
        session.rollback()
        raise
    finally:
        session.close()


def get_new_session():
    """
    获取新Session
    """
    session = session_factory()
    return session


if __name__ == "__main__":
    pass
