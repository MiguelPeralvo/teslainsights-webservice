#!/usr/bin/env python3
from datetime import datetime
import logging

import connexion
from connexion import NoContent

import orm

db_session = None


def get_historic_global_sentiments(
        from_ms_ago=864000000, from_created_epoch_ms=1532441907000,
        limit=1000, sample_rate=1.0, sentiment_type='all'
):
    q = db_session.query(orm.GlobalSentiment)
    delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
    utc_now = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

    # We'll pick up the most recent starting point
    starting_point = max(utc_now-from_ms_ago, from_created_epoch_ms)

    q = q.filter(
        orm.GlobalSentiment.created_at_epoch_ms >= starting_point,
        orm.GlobalSentiment.sentiment_type == sentiment_type,
    )

    # TODO: sample_rate
    if sample_rate<1:
        pass

    return [p.dump() for p in q][:limit]


logging.basicConfig(level=logging.INFO)
db_session = orm.init_db('sqlite:///:memory:')
app = connexion.FlaskApp(__name__)
app.add_api('swagger.yaml')

application = app.app


@application.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

if __name__ == '__main__':
    app.run(port=8080)