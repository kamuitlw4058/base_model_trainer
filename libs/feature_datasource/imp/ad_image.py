import logging
logger = logging.getLogger(__name__)
from libs.feature_datasource.imp.clickhouse_sql import  ClickHouseSQLDataSource

from libs.common.database.reader import get_mysql_reader
from libs.feature.model_udfs import image_vector as image_vector_udf

class AdImage(ClickHouseSQLDataSource):

    def __init__(self, name, sql_template,spark,parallel=False,**kwargs):
        super().__init__(name, sql_template, spark, parallel=parallel)
        self.model = None


    def get_dataframe(self):
        df =super().get_dataframe()
        return df


    def get_model(self):
        if self.model is None:
            from keras.applications.vgg16 import VGG16
            from keras.applications.vgg16 import preprocess_input
            from keras.models import Model
            from keras import layers

            from os import path
            import os
            # print(os.listdir())
            base_model = VGG16(weights="vgg16_weights_tf_dim_ordering_tf_kernels.h5")

            # print(base_model.summary())
            temp_model = base_model.get_layer('fc2').output
            temp_model = layers.Dense(1000, activation='linear', name='predictions')(temp_model)

            model = Model(inputs=base_model.input, outputs=temp_model)

            # print(model.summary())

            model.load_weights("vgg16_weights_tf_dim_ordering_tf_kernels.h5")

            self.model = model



    def get_image_vector(self,image_path):

        from keras.applications.vgg16 import VGG16
        from keras.applications.vgg16 import preprocess_input
        from keras.models import Model
        from keras import layers

        from keras.preprocessing.image import load_img
        self.get_model()

        # load an image from file
        image = load_img(image_path, target_size=(224, 224))

        from keras.preprocessing.image import img_to_array

        # convert the image pixels to a numpy array
        image = img_to_array(image)

        image = image.reshape((1, image.shape[0], image.shape[1], image.shape[2]))

        # prepare the image for the VGG model
        image = preprocess_input(image)

        yhat = self.model.predict(image)
        return yhat

    @staticmethod
    def download_image(adid):
        pass

    def produce_data(self,overwrite=False,df_handler=None,write_df=True):
        df = super().produce_data(overwrite=overwrite,write_df =False)
        # reader = get_mysql_reader()
        # image_vector_list = reader.read_sql("select AdId,ImageVector from features_image_vector")
        # exist_adid = image_vector_list.iloc[:,0:2].values
        # print(exist_adid)
        # adid_list = df.toPandas().iloc[:,0].values

        df =df.withColumn("image_vector", image_vector_udf("Bid_AdId"))
        # for i in  adid_list:
        #     AdImage.download_image(i)
        #     image_vector =  self.get_image_vector("tmp/"+ i + ".jpg")
        #     # if str(i) in exist_adid[:,0]:
        #     #     print(" adid is exist " + str(i))
        #     # else:
        #     #     print(" adid is not exist " + str(i) )

        return df
