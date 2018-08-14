import logging
logger = logging.getLogger(__name__)

import os
import numpy as np
from libs.dataio.utils import xlearning_progress
import pyarrow.parquet as pq


class DataGenerator:
    def __init__(self, file_str, input_dim, batch_size, epoch=1, prefix='.parquet'):
        self._batch_size = batch_size
        self._file_list = [os.path.join(file_str, i) for i in os.listdir(file_str) if i.endswith(prefix)]
        logger.info(f'data file number = {len(self._file_list)}')
        self._dim = input_dim
        self._epoch = epoch

    def sync_next_batch(self):
        total = self._epoch * len(self._file_list)
        progress = 0

        x = np.zeros(shape=[self._batch_size, self._dim], dtype=np.float32)
        y = np.zeros(shape=[self._batch_size, 1], dtype=np.float32)

        for epoch in range(self._epoch):
            for idx, f in enumerate(self._file_list):
                xlearning_progress(epoch, progress / total)
                progress += 1
                table = pq.read_table(f)
                for batch in table.to_batches(self._batch_size):
                    df = batch.to_pandas()
                    row_num = df.shape[0]
                    for i, row in df.iterrows():
                        x[i, row.feature_indices] = 1.0
                        y[i, 0] = row.is_clk

                    yield (x[:row_num], y[:row_num])
                    x.fill(0)
                    y.fill(0)

        xlearning_progress(epoch, 1.0)
