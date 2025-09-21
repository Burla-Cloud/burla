from burla import remote_parallel_map

import xgboost as xgb
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score


def train_model(params):

    X, y = make_classification(n_samples=100_000, n_features=60, n_informative=30)
    X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, stratify=y)

    model = xgb.XGBClassifier(tree_method="hist", n_jobs=80, eval_metric="auc", **params)
    model.fit(X_train, y_train, eval_set=[(X_valid, y_valid)], verbose=False)
    auc = roc_auc_score(y_valid, model.predict_proba(X_valid)[:, 1])

    print(f"Done training, [AUC={auc:.4f}] Hyperparameters: {params}")
    return {"auc": auc, "params": params}


parameter_grid = [
    {"n_estimators": n, "max_depth": d, "eta": e, "subsample": s, "colsample_bytree": c}
    for n in [200, 400]
    for d in [4, 8]
    for e in [0.05, 0.1]
    for s in [0.6, 1.0]
    for c in [0.8, 1.0]
]

results = remote_parallel_map(train_model, parameter_grid, func_cpu=80)

best = max(results, key=lambda r: r["auc"])
print("Best:", best)
