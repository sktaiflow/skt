from datetime import datetime, timedelta, date

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import Boolean
from utils.base import Base
from utils.sqlalchemy import provide_session


class SaturnTable(Base):
    __tablename__ = "saturn_tables"

    table_id = Column(String(1024), primary_key=True)
    sla_hours = Column(Integer, nullable=False)
    retention_days = Column(Integer, nullable=False)
    filter_rule = Column(String(4096))
    description = Column(String(4096))
    add_partition_sql = Column(String(4096))
    ddl_path = Column(String(1024))
    is_use = Column(Boolean)
    is_stable = Column(Boolean)
    request_type = Column(String(256))
    period = Column(String(256))
    target_systems = Column(String(256))
    dataset_id = Column(String(1024))
    source_location = Column(String(1024))
    requestor = Column(String(1024), nullable=False)
    bq_backfill_dts = Column(String(10000))

    def __repr__(self):
        return f"<Saturn Table: {self.table_id}>"

    @staticmethod
    @provide_session
    def add(table, session=None):
        if not table.table_id:
            raise Exception("table_id is required")
        if not table.sla_hours:
            raise Exception("sla_hour is required")
        if not table.retention_days:
            raise Exception("retention_days is required")
        session.add(table)

    @staticmethod
    @provide_session
    def get_all(session=None):
        table_list = session.query(SaturnTable).all()

        def process(table):
            t_dict = vars(table).copy()
            t_dict.pop("_sa_instance_state")
            t_dict["today_expected_partitions"] = table.today_expected_partitions()
            return t_dict

        to_dict = [process(t) for t in table_list]
        return to_dict

    @staticmethod
    @provide_session
    def delete(table_id, session=None):
        return session.query(SaturnTable).filter(SaturnTable.table_id == table_id).delete()

    @staticmethod
    @provide_session
    def update(table_id, attributes, session=None):
        t = session.query(SaturnTable).filter_by(table_id=table_id).first()
        for k, v in attributes.items():
            setattr(t, k, v)
        r = vars(t).copy()
        r.pop("_sa_instance_state", None)
        return r

    def apply_filter(self, dt):
        if self.filter_rule:
            return eval(self.filter_rule)
        return True

    def expected_partitions(self, until_kst, before_days):
        end_dt = (until_kst - timedelta(hours=self.sla_hours)).date()
        dts = [end_dt - timedelta(days=x) for x in range(0, before_days)]
        # filtered_dts = [dt for dt in dts if self.apply_filter(dt)]
        return [dt.strftime("%Y%m%d") for dt in dts]

    def today_expected_partitions(self):
        import pytz

        kst_now = datetime.utcnow().astimezone(pytz.timezone("Asia/Seoul"))
        return self.expected_partitions(kst_now, 1)
