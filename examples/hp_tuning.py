from burla import remote_parallel_map

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score


columns_to_train_on = [
    "Airline","Operating_Airline","Marketing_Airline_Network",
    "IATA_Code_Marketing_Airline","IATA_Code_Operating_Airline",
    "Origin","Dest","OriginState","DestState","OriginWac","DestWac",
    "OriginAirportID","DestAirportID","OriginCityMarketID","DestCityMarketID",
    "Distance","DistanceGroup","CRSElapsedTime",
    "DayOfWeek","Month","Quarter","DayofMonth","Year",
    "DepTimeBlk","ArrTimeBlk","CRSDepTime","CRSArrTime"
]


def train_model(params):
    print("Loading dataset ...")
    df = pd.read_csv("flight_delay_prediction_example.csv").dropna(subset=["ArrDel15"])
    
    y = df["ArrDel15"].astype(int)
    X = pd.get_dummies(df[columns_to_train_on], dummy_na=False)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    data_size_gb = (X_train.memory_usage(deep=True).sum() + y_train.memory_usage(deep=True)) / (1024**3)

    print(f"Training XGBClassifier on {format(len(df), ",")} rows ({data_size_gb:.1f}GB) using 80 CPUs ...")
    model = xgb.XGBClassifier(tree_method="hist", n_jobs=80, eval_metric="auc", **params)
    model.fit(X_train, y_train)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])

    print(f"Done training, [AUC={auc:.4f}] Hyperparameters: {params}")
    return {"auc": auc, "params": params}


parameter_grid = [
    {"n_estimators": n, "max_depth": d, "eta": e}
    for n in [300, 600, 900] for d in [4, 8] for e in [0.05, 0.11]
]

results = remote_parallel_map(train_model, parameter_grid, func_cpu=80)

best = max(results, key=lambda r: r["auc"])
print("Best:", best)
