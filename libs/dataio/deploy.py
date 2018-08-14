import logging
logger = logging.getLogger(__name__)

import os
from conf.conf import FTP_BACKUP_DIR, FTP_DIR
from libs.dataio.utils import set_mtime
from libs.env.shell import run_cmd


def put(local_src, remote_dst):
    cmd = 'rsync -atz {src} {dst}'.format(src=local_src, dst=remote_dst)
    run_cmd(cmd)
    logger.info('success send %s to %s', local_src, remote_dst)


def sender_all(job_id, version, file_list):
    logger.info('[%s] sending data to ftp', job_id)

    for file_name in file_list:
        # check and get file suffix
        base_name = os.path.basename(file_name)
        parts = base_name.split('.')
        if len(parts) < 2:
            raise RuntimeError(f'file name should has $name.$suffix, but we got {file_name}')
        suffix = parts[-1]
        logger.info('[%s] sending "%s" file', job_id, suffix)

        set_mtime(file_name, version)

        dsts = [
            f'{FTP_BACKUP_DIR}/{job_id}/', # copy file to dest dir (end with /)
            f'{FTP_DIR}/{job_id}.{suffix}'
        ]

        for dst in dsts:
            put(file_name, dst)
