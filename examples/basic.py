# from time import sleep
from burla import remote_parallel_map

import xgboost as xgb
import numpy as np


def my_function(x):

    X = np.array([[1, 2], [3, 4], [5, 6]])
    y = np.array([0, 1, 0])

    dtrain = xgb.DMatrix(X, label=y)

    params = {"objective": "binary:logistic", "verbosity": 0}
    model = xgb.train(params, dtrain, num_boost_round=2)

    print("Prediction:", model.predict(dtrain))


remote_parallel_map(my_function, list(range(160)))
