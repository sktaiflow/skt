from utils.base import Base
from utils.sqlalchemy import engine


def init_db():
    _initialize_db()


def _initialize_db():
    Base.metadata.create_all(engine)
