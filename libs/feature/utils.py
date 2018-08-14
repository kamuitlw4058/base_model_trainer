import logging
logger = logging.getLogger(__name__)

import os
cwd = os.getcwd()


def get_feature_name(category_feature, pipeline_model):
    feature_name = []
    fea_num = len(category_feature)
    for col_name, m in zip(category_feature, pipeline_model.stages[:fea_num]):
        for v in m.labels[:-1]:
            feature_name.append(f'{col_name}_{v.strip()}')

    return feature_name
