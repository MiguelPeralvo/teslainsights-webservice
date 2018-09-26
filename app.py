#!/usr/bin/env python3
import os
from datetime import datetime
import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import connexion
import orm
db_session = None


def get_historic_global_sentiments(
        from_ms_ago=86400000, from_created_epoch_ms=1532441907000,
        limit=1000, sample_rate=1.0, sentiment_type='all'
):
    q = db_session.query(orm.GlobalSentiment)
    delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
    utc_now = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

    # We'll pick up the most recent starting point
    starting_point = max(utc_now-from_ms_ago, from_created_epoch_ms)

    if sentiment_type=='all':
        in_all = [
            'social_teslamonitor', 'social_external_ensemble',
            'news_external_ensemble', 'global_external_ensemble'
        ]

        q = q.filter(
            orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
            orm.GlobalSentiment.sentiment_type.in_(in_all),
        )
    else:
        q = q.filter(
            orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
        )

    q = q.order_by(orm.GlobalSentiment.created_at_epoch_ms.desc())

    # The sampling is not random, we try to make the sample points equidistant in terms of points in the between.
    if sample_rate < 1:
        skip_rate = int(sample_rate * 100)
        return [p.dump() for i, p in enumerate(q) if (i % 100) < skip_rate][:limit]
    else:
        return [p.dump() for p in q][:limit]


db_host = os.getenv('AUTOMLPREDICTOR_DB_SERVER_IP', '127.0.0.1')
db_user = os.getenv('AUTOMLPREDICTOR_DB_SQL_USER', 'root')
db_password = os.getenv('AUTOMLPREDICTOR_DB_SQL_PASSWORD')
db_name = os.getenv('AUTOMLPREDICTOR_DB_NAME', 'automlpredictor_db_dashboard')
db_port = os.getenv('AUTOMLPREDICTOR_DB_PORT', 3306)
ssh_username = os.getenv('AUTOMLPREDICTOR_DB_SSH_USER', None)
ssh_password = os.getenv('AUTOMLPREDICTOR_DB_SSH_PASSWORD')
ssh = ssh_username is not None

db_session = orm.init_db(ssh, db_host, db_name, db_user, db_password, db_port, ssh_username, ssh_password, 'utf8mb4')
app = connexion.FlaskApp(__name__)
app.add_api('swagger.yaml')

application = app.app

@application.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

if __name__ == '__main__':
    app.run(port=8080)