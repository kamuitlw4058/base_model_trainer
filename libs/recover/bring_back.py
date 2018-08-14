import logging
import os
from datetime import datetime
from collections import namedtuple
from libs.tracker import Tracker
from conf.conf import FTP_BACKUP_DIR, FTP_DIR
from libs.dataio.utils import set_mtime
from libs.recover.persist import parse_desc
from libs.recover.utils import recopy_files
from libs.dataio.deploy import put
from libs.dataio.persist import write_desc

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def recover_desc(old_file, new_files, tracker, new_version, **kwargs):
    info = parse_desc(old_file)
    info.update(kwargs)
    info['version'] = '{:%Y-%m-%dT%H:%M:%S}'.format(new_version)
    info['start_time'] = '{:%Y-%m-%dT%H:%M:%S}'.format(new_version)
    info['end_time'] = '{:%Y-%m-%dT%H:%M:%S}'.format(new_version)

    for f in new_files:
        write_desc(f, **info)

    # write tracker
    for k, v in info.items():
        if k in ['account', 'vendor', 'sample_num', 'clk_sample_num', 'imp_sample_num']:
            tracker.set(k, int(v))
        elif k in ['version', 'start_time', 'end_time']:
            tracker.set(k, datetime.strptime(v, '%Y-%m-%dT%H:%M:%S'))
        else:
            tracker.set(k, v)

    os.remove(old_file)


def bring_back(job_id, old_version):
    old_version = datetime.strptime(old_version, '%Y-%m-%dT%H:%M')

    tracker = Tracker()
    tracker.job_id = job_id

    new_version = datetime.now()
    Recover = namedtuple('Recover', ['func', 'prefix', 'args'])
    account, vendor, os_id, audience_id = job_id.split('_')

    recovers = [
        Recover(func=recopy_files, prefix='weight', args={}),
        Recover(func=recopy_files, prefix='feature', args={}),
        Recover(func=recopy_files, prefix='he', args={}),
        Recover(func=recover_desc, prefix='desc', args={
            'new_version': new_version,
            'tracker': tracker,
            'account': account,
            'vendor': vendor,
            'os': {'1': 'android', '2': 'ios'}[os_id],
            'audience': {'1': 'ht', '2': 'rmkt'}[audience_id]
        }),
    ]

    # get old data from remote cache files
    for rcv in recovers:
        old_file = '{:%Y%m%d%H%M}.{}.{}'.format(old_version, job_id, rcv.prefix)
        src = os.path.join(FTP_BACKUP_DIR, job_id, old_file)
        put(src, cache_dir)

        new_files = [
            os.path.join(cache_dir, '{}.{}'.format(job_id, rcv.prefix)),
            os.path.join(cache_dir, '{:%Y%m%d%H%M}.{}.{}'.format(new_version, job_id, rcv.prefix))
        ]
        rcv.func(os.path.join(cache_dir, old_file), new_files, **rcv.args)

    # send out
    send_list = []
    for rcv in recovers:
        files = [
            (os.path.join(cache_dir, '{}.{}'.format(job_id, rcv.prefix)), FTP_DIR),
            (os.path.join(cache_dir, '{:%Y%m%d%H%M}.{}.{}'.format(new_version, job_id, rcv.prefix)),
             os.path.join(FTP_BACKUP_DIR, job_id)),
        ]
        send_list += files

    for f in send_list:
        set_mtime(f[0], new_version)

    for s, d in send_list:
        put(s, d)

    tracker.commit()
    logger.info("finish recover {} from {:%Y-%m-%dT%H:%M} to {:%Y-%m-%dT%H:%M}".format(job_id, old_version, new_version))


if __name__ == '__main__':
    job_id = '109_24_2_1'
    version = '2017-09-25T11:39'
    bring_back(job_id, version)

