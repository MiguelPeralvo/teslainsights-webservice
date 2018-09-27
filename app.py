#!/usr/bin/env python3
import os
from datetime import datetime
import traceback
import sqlalchemy.ext
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import connexion
import orm
db_session = None
MAX_RETRIES=1

def get_historic_global_sentiments_inner(
        from_ms_ago=86400000, from_created_epoch_ms=1532441907000,
        limit=1000, sample_rate=1.0, sentiment_type='all', retry_count=0
):
    global db_session
    final_dataset = []

    try:
        q = db_session.query(orm.GlobalSentiment)
        delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
        utc_now = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

        # We'll pick up the most recent starting point
        starting_point = max(utc_now - from_ms_ago, from_created_epoch_ms)

        if sentiment_type == 'all':
            sentiment_types = [
                'social_teslamonitor', 'social_external_ensemble',
                'news_external_ensemble', 'global_external_ensemble'
            ]
        elif sentiment_type == 'teslamonitor':
            sentiment_types = [
                'social_teslamonitor',
            ]
        elif sentiment_type == 'external':
            sentiment_types = [
                'social_external_ensemble', 'news_external_ensemble', 'global_external_ensemble'
            ]
        else:
            sentiment_types = [sentiment_type]

        q = q.filter(
            orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
            orm.GlobalSentiment.sentiment_type.in_(sentiment_types),
        )
        q = q.order_by(orm.GlobalSentiment.created_at_epoch_ms.desc())

        # The sampling is not random, we try to make the sample points equidistant in terms of points in the between.
        if sample_rate < 1:
            skip_rate = int(sample_rate * 100)
            final_dataset = [p.dump() for i, p in enumerate(q) if (i % 100) < skip_rate][:limit]
        else:
            final_dataset = [p.dump() for p in q][:limit]

    except sqlalchemy.exc.OperationalError:
        db_session.rollback()
        db_session.remove()

        # Restore the session and retry.
        session_reconnect()

        if retry_count < MAX_RETRIES:
            get_historic_global_sentiments_inner(from_ms_ago, from_created_epoch_ms, limit, sample_rate, sentiment_type, retry_count+1)
        else:
            raise
    except:
        db_session.rollback()
        logger.error(f'An error occurred when managing the query for retrieving global sentiment: {traceback.format_exc()}')
        raise

    return final_dataset

def get_historic_global_sentiments(
        from_ms_ago=86400000, from_created_epoch_ms=1532441907000,
        limit=1000, sample_rate=1.0, sentiment_type='all'
):
    return get_historic_global_sentiments_inner(from_ms_ago, from_created_epoch_ms, limit, sample_rate, sentiment_type)

def session_reconnect():
    global db_session
    db_session = orm.init_db(ssh, db_host, db_name, db_user, db_password, db_port, ssh_username, ssh_password, 'utf8mb4')


db_host = os.getenv('AUTOMLPREDICTOR_DB_SERVER_IP', '127.0.0.1')
db_user = os.getenv('AUTOMLPREDICTOR_DB_SQL_USER', 'root')
db_password = os.getenv('AUTOMLPREDICTOR_DB_SQL_PASSWORD')
db_name = os.getenv('AUTOMLPREDICTOR_DB_NAME', 'automlpredictor_db_dashboard')
db_port = os.getenv('AUTOMLPREDICTOR_DB_PORT', 3306)
ssh_username = os.getenv('AUTOMLPREDICTOR_DB_SSH_USER', None)
ssh_password = os.getenv('AUTOMLPREDICTOR_DB_SSH_PASSWORD')
ssh = ssh_username is not None


session_reconnect()
app = connexion.FlaskApp(__name__)
app.add_api('swagger.yaml')

application = app.app

@application.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

if __name__ == '__main__':
    app.run(port=8080)