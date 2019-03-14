from libs.utils.sys_cmd import exec_sync
from libs.conf.ftp import FTP_USER, FTP_PASSWORD, FTP_SITE
from libs.utils.sys_cmd import exec_sync
import ftplib
import os

def _get_client():
    """
    @:rtype ftplib.FTP
    """
    ftp_client = ftplib.FTP(FTP_SITE)  # 实例化FTP对象
    ftp_client.login(FTP_USER, FTP_PASSWORD)  # 登录    cmd = f"lftp -u {FTP_USER},{FTP_PASSWORD} {FTP_SITE} -e ls"
    print(ftp_client.getwelcome())
    return ftp_client

def _close_client(client):
    """
    @type client:ftplib.FTP
    """
    try:
        client.quit()
    except Exception as e:
        print(e)
        client.close()
    return



def ls(path,is_dump=False):
    client = _get_client()
    client.cwd(path)
    if is_dump:
        client.dir()
    files =client.nlst()
    _close_client(client)
    return files




def put(local_file_path,remote_dir):
    client = _get_client()
    client.cwd(remote_dir)
    local_file_name = os.path.basename(local_file_path)
    bufsize = 1024 * 10 # 设置缓冲器大小
    fp = open(local_file_path, 'rb')
    client.storbinary('STOR ' + local_file_name, fp, bufsize)
    _close_client(client)

def get(remote_file,local_file):
    client = _get_client()
    remote_filedir = os.path.dirname(remote_file)
    remote_filename = os.path.basename(remote_file)
    client.cwd(remote_filedir)
    if os.path.isdir(local_file):
        local_file = local_file + "/" + remote_filename
    client.retrbinary('RETR ' + remote_filename, open(local_file, 'wb').write)
    _close_client(client)



def delete(remote_file_path):
    client = _get_client()
    filename = os.path.basename(remote_file_path)
    filedir = os.path.dirname(remote_file_path)
    client.cwd(filedir)
    client.delete(filename)
    _close_client(client)

def cat(remote_file_path):
    filename = os.path.basename(remote_file_path)
    filedir = os.path.dirname(remote_file_path)
    get(remote_file_path,"../tmp/" + filename)
    status,logs =exec_sync("cat ../tmp/" + filename)
    print(logs)
    return  logs





if __name__ == "__main__":
    pass

    #delete('anti_fraud/client.py')

    #ls('anti_fraud/anti_fraud_remit',is_dump=True)
    logs =  cat('anti_fraud/anti_fraud_remit/anti_fraud_remit_current.txt')
    if logs is not None and len(logs) > 0:
        version_file =  logs[0].decode()
        cat('anti_fraud/anti_fraud_remit/' + version_file)




    #get("anti_fraud/anti_fraud_remit/anti_fraud_remit_20160810205333.tar.gz","../tmp")
    # get("anti_fraud/anti_fraud_remit/")
    #put('whoserver/client.py','anti_fraud')

    #anti_fraud_remit_20181210024121


    #get('anti_fraud/anti_fraud_20161019_1430_patch.tar.gz','../tmp/data.tar.gz')
    # files = ls('anti_fraud')
    # if 'client.py' in files:
    #     print('client exist')
    # else:
    #     print('client not exist')

