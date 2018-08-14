import subprocess


def run_cmd(cmd, env=None):
    proc = subprocess.Popen(cmd, shell=True, env=env)
    ret = proc.wait()
    if ret != 0:
        raise RuntimeError(f'fail to run {cmd}')
