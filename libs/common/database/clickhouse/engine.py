import sqlalchemy
import random
from zamplus_common.conf.clickhouse import CH_HOSTS_PORT
from zamplus_common.conf.clickhouse import pandas_url



def create_engine(database):
    engine_url = pandas_url.format(random.choice(CH_HOSTS_PORT), database)
    engine = sqlalchemy.create_engine(engine_url)
    return engine


