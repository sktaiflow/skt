from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from utils.base import Base
from utils.sqlalchemy import provide_session


class SwingPKTable(Base):
    __tablename__ = "swing_pk_table"

    table_id = Column(String(1024), primary_key=True)
    column_name = Column(String(1024), primary_key=True)
    column_position = Column(Integer)
    rowkey_position = Column(Integer)
    comments = Column(String(4096))

    def __repr__(self):
        return f"<Swing PK Table: {self.table_id}>"

    @staticmethod
    @provide_session
    def add(table, session=None):
        if not table.table_id:
            raise Exception("table_id is required")
        if not table.column_position:
            raise Exception("column_position is required")
        if not table.column_name:
            raise Exception("column_name is required")
        if not table.rowkey_position:
            raise Exception("rowkey_position is required")
        session.add(table)

    @staticmethod
    @provide_session
    def get_all(session=None):
        table_list = session.query(SwingPKTable).all()

        def process(table):
            t_dict = vars(table).copy()
            t_dict.pop("_sa_instance_state")
            return t_dict

        to_dict = [process(t) for t in table_list]
        return to_dict

    @staticmethod
    @provide_session
    def get_pk_table(table_id, session=None):
        def row2dict(row):
            d = {}
            for column in row.__table__.columns:
                d[column.name] = str(getattr(row, column.name))

            return d

        query_result = session.query(SwingPKTable).filter_by(table_id=table_id).all()

        return [row2dict(r) for r in query_result]

    @staticmethod
    @provide_session
    def delete(table_id, session=None):
        return session.query(SwingPKTable).filter(SwingPKTable.table_id == table_id).delete()

    @staticmethod
    @provide_session
    def update(table_id, attributes, session=None):
        t = session.query(SwingPKTable).filter_by(table_id=table_id).first()
        for k, v in attributes.items():
            setattr(t, k, v)
        r = vars(t).copy()
        r.pop("_sa_instance_state", None)
        return r
