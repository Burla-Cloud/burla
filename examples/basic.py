# from time import sleep
from burla import remote_parallel_map

import tensorflow as tf
import numpy as np

import xgboost as xgb


def my_function(x):

    X = np.array([[1, 2], [3, 4], [5, 6]], dtype=np.float32)
    y = np.array([0, 1, 0], dtype=np.float32)

    model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="sigmoid", input_shape=(2,))])

    model.compile(optimizer="adam", loss="binary_crossentropy")

    model.fit(X, y, epochs=10, verbose=0)

    print("Prediction:", model.predict(X).flatten())

    dtrain = xgb.DMatrix(X, label=y)

    # simple booster config
    params = {"objective": "binary:logistic", "verbosity": 0}
    model = xgb.train(params, dtrain, num_boost_round=2)

    print("Prediction:", model.predict(dtrain))


remote_parallel_map(my_function, list(range(2)))
