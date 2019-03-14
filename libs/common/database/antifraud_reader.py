from zamplus_common.datasource.mysql.engine import create_engine as mysql_create_engine
from zamplus_common.datasource.reader import Reader

database="db_antifraud"
host="172.22.57.4"
user="user_antifraud"
password="H2bG43nU"


def get_mysql_reader():
    engine = mysql_create_engine(database,host,user,password)
    print(database)
    reader = Reader(engine,database)


    return reader