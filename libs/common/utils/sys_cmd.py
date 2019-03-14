import subprocess


def exec_sync(shell_cmd,verbose=False,logs_type='str',stderr2stdout =True):

    logs =[]
    if stderr2stdout:
        p=subprocess.Popen(shell_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    else:
        p=subprocess.Popen(shell_cmd,shell=True,stdout=subprocess.PIPE)
    status = None
    while status is None:
        status = p.poll()
        log_line = p.stdout.readline()
        log_line =log_line[0:-1].decode()
        logs.append(log_line)
        if verbose:
            print(log_line)

    log_lines = p.stdout.readlines()
    for log_line in log_lines:
        log_line = log_line[0:-1].decode()
        logs.append(log_line)
        if verbose:
            print(log_line)

    if logs_type == 'str':
        logs_str = ""
        for log in logs:
            logs_str += log + "\n"
        return status,logs_str
    else:
        return status,logs




def cat(file_path,verbose=False):
    status,logs =exec_sync("cat " + file_path)
    if verbose:
        print(logs)
    return  logs



