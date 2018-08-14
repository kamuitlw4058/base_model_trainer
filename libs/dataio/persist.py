import logging
logger = logging.getLogger(__name__)


def write_desc(filename, job):
    meta, runtime = job.meta, job.runtime
    # job summary
    job_summary = {
        'version': runtime.end_time,
        'account': meta.get('account', ''),
        'vendor': meta.vendor,
        'os': meta.os,
        'audience': meta.get('audience', ''),
        'train_auc': runtime.train_auc,
        'test_auc': runtime.test_auc,
        'input_dim': runtime.input_dim,
        'sample_num': runtime.imp_sample_num,
        'clk_sample_num': runtime.clk_sample_num,
        'imp_sample_num': runtime.imp_sample_num,
        'start_time': runtime.start_time,
        'end_time': runtime.end_time,
        'status': runtime.status
    }

    f = open(filename, mode='w', encoding='utf-8')
    for k, v in job_summary.items():
        try:
            f.write(f'{k}\t{v}\n')
        except Exception as e:
            logger.error('[%s] fail write %s -> %f', job.id, k, v)
            raise e
    f.close()

    logger.info('[%s] success, write %s file', job.id, filename)
