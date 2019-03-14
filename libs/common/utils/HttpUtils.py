import logging
logger = logging.getLogger(__name__)

import urllib3

url = "http://www.baidu.com"

url = 'http://172.22.24.154:8017/filter'

urllib_pool_manager = urllib3.PoolManager()



def get(url):
    response =None
    try:
        response = urllib_pool_manager.request('get', url)
    except Exception as e:
        logger.exception(e)
    if response and response.data:
        return response.data.decode()
    else:
        return None

def post(url,data):
    response =None
    try:
        response = urllib_pool_manager.request('post',
                                               url,
                                               body=data
                                               )
    except Exception as e:
        logger.exception(e)
    if response and response.data:
        return response.data.decode()
    else:
        return None

