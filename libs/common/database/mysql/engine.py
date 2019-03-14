import sqlalchemy
import random
from zamplus_common.conf.mysql import default_config

def get_default_database():
    return default_config.get("database")

mysql_url= 'mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8'

def create_engine(database=None,host=None,user=None,password=None):
    if not host or not user or not password or not database:
        config = default_config
    else:
        config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
        }
    #print(mysql_url.format(**config))
    engine = sqlalchemy.create_engine(mysql_url.format(**config))
    return engine
