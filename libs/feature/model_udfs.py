from pyspark.sql.functions import *

from keras.applications.vgg16 import VGG16
from keras.models import Model
from keras import layers
from keras.applications.vgg16 import preprocess_input
from keras.preprocessing.image import load_img

import  os
def get_image_vector_model():
    model_path = "vgg16_weights_tf_dim_ordering_tf_kernels.h5"

    if os.path.exists(model_path):
        model_path = model_path
    else:
        #print(os.listdir("weights"))
        model_path = "./weights/" + model_path


    base_model = VGG16(weights=model_path)

    # print(base_model.summary())
    temp_model = base_model.get_layer('fc2').output
    temp_model = layers.Dense(1000, activation='linear', name='predictions')(temp_model)

    model = Model(inputs=base_model.input, outputs=temp_model)

    model.load_weights(model_path)

    return model

model = get_image_vector_model()


@udf()
def image_vector(dt):
    image_path = "./image/" + str(dt) + ".jpg"
    vector = get_image_vector(image_path)
    return vector


def get_image_vector(image_path):


    if not os.path.exists(image_path):
        return None

    # load an image from file
    image = load_img(image_path, target_size=(224, 224))

    from keras.preprocessing.image import img_to_array

    # convert the image pixels to a numpy array
    image = img_to_array(image)

    image = image.reshape((1, image.shape[0], image.shape[1], image.shape[2]))

    # prepare the image for the VGG model
    image = preprocess_input(image)

    yhat = model.predict(image)
    return [float(i) for i in yhat[0]]