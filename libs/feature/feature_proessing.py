import logging
logger = logging.getLogger(__name__)

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StringIndexerModel
from pyspark.ml import Pipeline
from pyspark.sql import functions
from libs.feature.multi_category_encoder import MultiCategoryEncoder, MultiCategoryEncoderModel

from libs.feature.processing.onehot import OneHotProcessing
from libs.feature.processing.number import IntProcessing
from libs.feature.processing.number import DoubleProcessing
from libs.feature.processing.multi_value_category import MultiValueCategoryProcessing
from libs.feature.processing.vector import VectorProcessing
from libs.feature.udfs import vector_indices,vector_values

processing_dict ={
    IntProcessing.get_name(): IntProcessing,
    DoubleProcessing.get_name(): DoubleProcessing,
    VectorProcessing.get_name(): VectorProcessing,
    OneHotProcessing.get_name():OneHotProcessing,
    MultiValueCategoryProcessing.get_name():MultiValueCategoryProcessing,

}


conf_processing_col_name ="col_name"



def _extract_vocabulary(stages,stages_output_dict,dataframe_features,vector_cols):
    def _build_dict(name, values,opt):
        return {
            'name': name,
            'value': ['' if v == 'nan' else v for v in values],
            'opt':opt
        }

    vocabulary_list = []

    for col in dataframe_features:
        vocabulary_list.append(_build_dict(col, [col],'orig'))

    for vector_cols in vector_cols:
        vector_cols_list = []
        for i in range(vector_cols['size']):
            vector_cols_list.append(i)
        vocabulary_list.append(_build_dict(f"{vector_cols['col']}", vector_cols_list, 'orig'))

    for stage in stages:
        #print(f"stage:{stage},type:{type(stage)}")
        if isinstance(stage, MultiCategoryEncoderModel):
            vocabulary_list.append(_build_dict(stage.getInputCol(), stage.getVocabulary(),"multi-one-hot"))
        if isinstance(stage, StringIndexerModel):
            (col,processing) =stages_output_dict[stages.index(stage)]
            vocabulary_list.append(_build_dict(col, stage.labels,'one-hot'))


    return vocabulary_list

def _dump_vocabulary(vocabulary):
    for feature in vocabulary:
        #logger.info(f"feature:{feature['name']} len:{len(feature['value'])} values:{feature['value']}")
        logger.info(f"feature:{feature['name']} len:{len(feature['value'])}")

def feature_dim(vocabulary):
    dim = 0
    for feature in vocabulary:
        dim += len(feature['value'])
    return dim




def get_result(df,lable_col):

    df = df.withColumn('feature_indices', vector_indices('feature'))
    df = df.withColumn('feature_values', vector_values('feature'))
    schema = [lable_col, 'feature_indices','feature_values']
    # others = [c for c in df.columns if c not in schema]
    res = (df
           # .select(['is_clk', 'feature'])
           .orderBy(functions.rand())
           .select(schema)
           )
   # res.show(10)

    return res


def get_features_vocabulary(vocabulary):
    l = []
    idx = 0
    for fe in vocabulary:
        name = fe['name']
        for v in fe['value']:
            d = {}
            d["name"] = f'{name}_{v}'
            d["index"] = idx
            d["opt"] = fe["opt"]
            l.append(d)
            idx += 1
    return l


def processing(train, test, processing_conf):
    logger.info(f'train orig columns {train.schema.names}')
    processing_col_conf = processing_conf["cols"]
    processing_col_dict={}
    total_cols = []

    for k,v in processing_dict.items():
        processing_col_dict[k] = []


    processing_cols_check_dict ={}
    processing_cols_check_func_list =[]

    for processing_col in processing_col_conf:
        processing_name = processing_col['processing']
        processing_col_name = processing_col[conf_processing_col_name]
        processing_col_list = processing_col_dict.get(processing_name)
        if processing_col_list is not None:
            processing_cols_check_func_list.append(functions.countDistinct(processing_col_name).alias(processing_col_name + '_cnt'))
            processing_cols_check_dict[processing_col_name] = (processing_name,processing_col_name + "_cnt")
            # if train.agg(functions.countDistinct(processing_col_name).alias('cnt')).collect()[0].cnt > 1:
            #
            #     processing_col_list.append(processing_col_name)
            # else:
            #     logger.warning(f'processor:{processing_name} col:{processing_col_name} count <=1 drop it')
        else:
            logger.warning(f'processor:{processing_name} col:{processing_col_name} not register processer drop it')

    cnts =  train.agg(*processing_cols_check_func_list).collect()[0]

    for processing_col_name,(processing_name, col_cnt_name) in processing_cols_check_dict.items():
        col_cnt = eval(f"cnts.{col_cnt_name}")
        if col_cnt > 1:
            processing_col_list = processing_col_dict.get(processing_name)
            if processing_col_list is not None:
                processing_col_list.append(processing_col_name)
        else:
            logger.warning(f'processor:{processing_name} col:{processing_col_name} count <=1 drop it')





    logger.info(f'processing_col_dict: {processing_col_dict}')

    #train_count, test_count = train.count(), test.count()

    stages_output_dict ={}
    stages = []
    total_output_cols = []

    dataframe_cols = []
    vector_cols =[]
    for k, v in processing_col_dict.items():
        if len(v) > 0:
            processing = processing_dict.get(k)
            if processing.get_type() == "pipline":
                processing_stage,output_stage,output_cols  =processing.get_processor(v)
                stages += processing_stage
                # print(f"processing_stage:{processing_stage}")
                # print(f"output_cols:{output_cols}")
                # print(f"stages:{stages}")

                for stage,input_col in output_stage:
                    stages_output_dict[stages.index(stage)] = (input_col,processing_dict.get(k))
            elif processing.get_type() == "dataframe":
                processor = processing.get_processor(v)
                train,cols = processor(train,v)
                test,cols =processor(test,v)
                output_cols = cols
                dataframe_cols += cols
            elif processing.get_type() == "vector":
                processor = processing.get_processor(v)
                train,cols,size = processor(train,v)
                test,cols,size =processor(test,v)
                output_cols = cols
                vector_cols += size


            total_output_cols += output_cols


    assembler = VectorAssembler(inputCols=total_output_cols, outputCol='feature')
    stages.append(assembler)

    #logger.info(f"stages_output_dict:{stages_output_dict}")
    #logger.info(f"stages:{stages}")
    logger.info(f"total_output_cols:{total_output_cols}")

    pipeline = Pipeline(stages=stages)

    model = pipeline.fit(train)

    vocabulary = _extract_vocabulary(model.stages,stages_output_dict,dataframe_cols,vector_cols)
    #logger.info(f"vocabulary:{vocabulary}")
    _dump_vocabulary(vocabulary)

    train_tranfrom =model.transform(train)
    train_tranfrom.show(10)

    # 这边使用训练集的转换模型去转换测试集。
    test_tranfrom = model.transform(test)

    v = train_tranfrom.select('feature').take(1)[0].feature
    logger.info('feature vector size = %d', v.size)
    feature_dim_number = feature_dim(vocabulary)
    if v.size != feature_dim(vocabulary):
        raise RuntimeError(f'feature vector size not match,'
                           f' real({v.size}) != calc({feature_dim_number})')


    train_res = get_result(train_tranfrom, processing_conf["label"])
    test_res = get_result(test_tranfrom, processing_conf["label"])


    return train_res, test_res,vocabulary,feature_dim(vocabulary)