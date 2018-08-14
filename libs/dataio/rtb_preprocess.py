import logging
logger = logging.getLogger(__name__)

import pandas as pd
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType
from libs.feature import udfs

_edu_mask = pd.DataFrame({
    'Education'    : ['',    '小学',            '中学',            '高中',           '高中(中专)及以下', '高中中专及以下',   '高中（中专）及以下', '大学大专及以上', '本科',      '本科及大专', '硕士', '硕士及以上', '博士', '博士及以上'],
    'education_new': ['nan', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下', '高中(中专)及以下',  '本科及大专',     '本科及大专', '本科及大专', '硕士', '硕士',      '博士', '博士']
})

_age_mask = pd.DataFrame({
    'Age'    : ['',    '0-18', '19-23', '24-30', '31-40', '41-50', '50以上', '51-999'],
    'age_new': ['nan', '0-18', '19-23', '24-30', '31-40', '41-50', '51-199', '51-199']
})

_gender_mask = pd.DataFrame({
    'Gender'    : ['', 'gender', '女', '男'],
    'gender_new': ['nan', 'nan', '女', '男']
})


def clean_data( raw, spark):

    for mask, join_col, temp_col in [
        (_edu_mask, 'Education', 'education_new'),
        (_age_mask, 'Age', 'age_new'),
        (_gender_mask, 'Gender', 'gender_new')
    ]:
        _mask = spark.createDataFrame(mask)
        raw = raw.join(broadcast(_mask), join_col, how='left_outer').drop(join_col).withColumnRenamed(temp_col, join_col)

    # last to process nan and invalid string value
    mask = {k.name: 'nan' for k in raw.schema if isinstance(k.dataType, StringType)}
    raw = raw.na.fill(mask).replace(['', '\\n', '\\n\\n'], 'nan')

    return raw

def expend_fields(raw, ext_feature):
    # append fields
    from functools import reduce
    ext_dict = 'ext_dict'
    opts = [
        ('weekday', udfs.weekday('ts')),
        ('is_weekend', udfs.is_weekend('ts')),
        (ext_dict, udfs.to_ext_dict('ext_key', 'ext_value')),
    ]

    raw = reduce(lambda d, args: d.withColumn(*args), opts, raw)

    # extract cap feature & bidding feature from ext_dict
    raw = reduce(lambda df, c: df.withColumn(c, df[ext_dict].getItem(c)), ext_feature, raw)

    return raw.drop(ext_dict, 'ext_key', 'ext_value')