from sqlalchemy import Column, String, BigInteger, Numeric, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

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

def init_db(uri):
    engine = create_engine(uri, convert_unicode=True)
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)
    return db_session