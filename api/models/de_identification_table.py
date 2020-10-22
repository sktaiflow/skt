from sqlalchemy import Column
from sqlalchemy import String
from utils.base import Base
from utils.sqlalchemy import provide_session


class DeIdentificationTable(Base):
    __tablename__ = "de_identification_table"

    platform_id = Column(String(256), primary_key=True)
    table_id = Column(String(1024), primary_key=True)
    column_id = Column(String(1024), primary_key=True)
    identification_type = Column(String(256))
    comments = Column(String(4096))

    def __repr__(self):
        return f"<De-Identification Table: {self.table_id}>"

    @staticmethod
    @provide_session
    def add(table, session=None):
        if not table.platform_id:
            raise Exception("platform_id is required")
        if not table.table_id:
            raise Exception("table_id is required")
        if not table.column_id:
            raise Exception("column_id is required")
        if not table.identification_type:
            raise Exception("identification_type is required")
        session.add(table)

    @staticmethod
    @provide_session
    def get_all(session=None):
        table_list = session.query(DeIdentificationTable).all()

        def process(table):
            t_dict = vars(table).copy()
            t_dict.pop("_sa_instance_state")
            return t_dict

        to_dict = [process(t) for t in table_list]
        return to_dict

    @staticmethod
    @provide_session
    def get_table(table_id, session=None):

        def row2dict(row):
            d = {}
            for column in row.__table__.columns:
                d[column.name] = str(getattr(row, column.name))

            return d

        query_result = session.query(DeIdentificationTable).filter_by(table_id=table_id).all()

        return [row2dict(r) for r in query_result]

    @staticmethod
    @provide_session
    def delete(table_id, session=None):
        return session.query(DeIdentificationTable).filter_by(table_id=table_id).delete()

    @staticmethod
    @provide_session
    def update(table_id, attributes, session=None):
        t = session.query(DeIdentificationTable).filter_by(table_id=table_id).first()
        for k, v in attributes.items():
            setattr(t, k, v)
        r = vars(t).copy()
        r.pop("_sa_instance_state", None)
        return r