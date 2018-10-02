#!/usr/bin/env python3
import json
import os
import random
random.seed(27)
from datetime import datetime
import sqlalchemy.ext
import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import connexion
import orm
db_session = None
MAX_RETRIES = 1

def get_historic_global_sentiments_inner(
        from_ms_ago=86400000, from_created_epoch_ms=1532441907000,
        limit=1000, downsample_freq=None, sample_rate=1.0, sentiment_type='all', retry_count=0
):
    global db_session
    final_dataset = []
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

    try:
        if downsample_freq:
            # We'll sample using the created_at_epoch_ms field, so the sampling is consistent across invocations. Could introduce some bias.
            # Useful for visualisation. Discouraged for analysis.
            # orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
            # orm.GlobalSentiment.sentiment_type.in_(sentiment_types),

            sentiment_types_sql = "', '".join(sentiment_types)

            sql_query = f"""
                SELECT sentiment_type, sentiment_seconds_back, 
                ROUND(created_at_epoch_ms/(1000*{downsample_freq})) AS bin,
                ROUND(AVG(created_at_epoch_ms)) AS created_at_epoch_ms,
                AVG(sentiment_absolute) AS sentiment_absolute,
                AVG(sentiment_normalized)  AS sentiment_normalized,
                MIN(created_at_epoch_ms) AS min_created_at_epoch_ms,
                MAX(created_at_epoch_ms) AS max_created_at_epoch_ms
                FROM analysis_global_sentiment 
                WHERE created_at_epoch_ms >= {starting_point}
                AND sentiment_type IN ('{sentiment_types_sql}')
                GROUP BY sentiment_type, sentiment_seconds_back, bin
                ORDER BY created_at_epoch_ms DESC             
            """

            results = db_session.connection().execute(sql_query)

            final_dataset = [
                {
                    'sentiment_type': row[0],
                    'sentiment_seconds_back': row[1],
                    'created_at_epoch_ms': row[3],
                    'sentiment_absolute': float(row[4]),
                    'sentiment_normalized': row[5],
                    'min_created_at_epoch_ms': row[6],
                    'max_created_at_epoch_ms': row[7],
                } for row in results
            ][:limit]

        else:
            q = db_session.query(orm.GlobalSentiment)

            q = q.filter(
                orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
                orm.GlobalSentiment.sentiment_type.in_(sentiment_types),
            )

            q = q.order_by(orm.GlobalSentiment.created_at_epoch_ms.desc())

            # The sampling is not random, we try to make the sample points equidistant in terms of points in the between.
            if sample_rate < 1:
                # pick_up_rate = (1/sample_rate).as_integer_ratio()
                # skip_rate = int(sample_rate * 10000)
                dataset = [(i, p.dump()) for i, p in enumerate(q)]
                pre_limit_dataset = random.sample(dataset, int(sample_rate*len(dataset)))
                # pre_limit_dataset = [p for i, p in enumerate(dataset) if (i % 10000) < skip_rate]
                final_dataset = [value for index, value in sorted(pre_limit_dataset[:limit], key=lambda tup: tup[0])]
            else:
                final_dataset = [p.dump() for p in q][:limit]


    except sqlalchemy.exc.OperationalError:
        db_session.rollback()
        db_session.remove()
        logger.warning(f'Recreating session because of issue with previous session: {traceback.format_exc()}')

        # Restore the session and retry.
        session_reconnect()

        if retry_count < MAX_RETRIES:
            final_dataset = get_historic_global_sentiments_inner(
                from_ms_ago, from_created_epoch_ms, limit, sample_rate, sentiment_type, retry_count+1
            )
        else:
            raise
    except:
        db_session.rollback()
        logger.error(f'An error occurred when managing the query for retrieving global sentiment: {traceback.format_exc()}')
        raise

    return final_dataset

def get_historic_global_sentiments(
        from_ms_ago=86400000, from_created_epoch_ms=1532441907000,
        limit=1000, downsample_freq=None, sample_rate=1.0, sentiment_type='all'
):
    return get_historic_global_sentiments_inner(from_ms_ago, from_created_epoch_ms, limit, downsample_freq, sample_rate, sentiment_type)

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