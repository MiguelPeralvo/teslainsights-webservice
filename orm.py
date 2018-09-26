from sqlalchemy import Column, String, BigInteger, Numeric, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

import sys

sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


Base = declarative_base()

class GlobalSentiment(Base):
    __tablename__ = 'analysis_global_sentiment'
    sentiment_type = Column(String(30), primary_key=True)
    sentiment_seconds_back = Column(BigInteger(), primary_key=True)
    created_at_epoch_ms = Column(BigInteger(), primary_key=True)
    sentiment_absolute = Column(Numeric(precision=10, scale=3, asdecimal=True))
    sentiment_normalized = Column(Numeric(precision=5, scale=3, asdecimal=True), nullable=True)

    def dump(self):
        return dict([(k, v) for k, v in vars(self).items() if not k.startswith('_')])

def init_db(ssh, db_host, database_name, db_user, db_password, db_port, ssh_username, ssh_password, charset='utf8mb4'):
    if ssh:
        ssh_server = SSHTunnelForwarder(
            ssh_address_or_host=(db_host, 22),
            ssh_password=ssh_password,
            ssh_username=ssh_username,
            remote_bind_address=('127.0.0.1', db_port))

        ssh_server.start()
        local_bind_port = ssh_server.local_bind_port
        engine = create_engine(
            f'mysql+mysqlconnector://{db_user}:{db_password}@127.0.0.1:{local_bind_port}/{database_name}?charset={charset}',
            convert_unicode=True
        )

    else:
        engine = create_engine(
            f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}?charset={charset}',
            convert_unicode=True
        )

    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine, pool_recycle=3600))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)
    return db_session