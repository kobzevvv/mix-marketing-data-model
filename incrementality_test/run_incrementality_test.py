import pandas as pd
import numpy as np
import yaml
import os
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import r2_score
from datetime import timedelta

def run_incrementality_test(df, test_start, test_end, pre_test_days=30, saturation=0.25, alpha_grid=[0.1, 1.0, 10.0]):
    df['date'] = pd.to_datetime(df['date'])

    test_start_date = pd.to_datetime(test_start)
    test_end_date = pd.to_datetime(test_end)
    start_date = test_start_date - timedelta(days=pre_test_days)

    df_window = df[(df['date'] >= start_date) & (df['date'] <= test_end_date)].copy()

    if df_window.empty or len(df_window) < 10:
        raise ValueError("Not enough data for regression.")

    df_window['saturated_spend'] = df_window['spend'] ** (1 - saturation)

    X = df_window[['saturated_spend']]
    y = df_window['revenue']

    model = RidgeCV(alphas=alpha_grid, cv=TimeSeriesSplit(n_splits=5))
    model.fit(X, y)

    y_pred = model.predict(X)
    return {
        'r2': round(r2_score(y, y_pred), 3),
        'saturation': saturation,
        'roas_coefficient': round(model.coef_[0], 4),
        'ridge_alpha': model.alpha_
    }

if __name__ == "__main__":
    if os.path.exists("meta_uk_daily.csv"):
        df = pd.read_csv("meta_uk_daily.csv")
        params = {
            "test_start": "2024-05-01",
            "test_end": "2024-06-01",
            "pre_test_days": 30,
            "saturation": 0.25
        }
    else:
        with open("incrementality_test_input.yaml", "r") as f:
            yaml_data = yaml.safe_load(f)
        df = pd.DataFrame(yaml_data["daily_data"])
        params = {
            "test_start": yaml_data["test_start"],
            "test_end": yaml_data["test_end"],
            "pre_test_days": yaml_data.get("pre_test_days", 30),
            "saturation": yaml_data.get("saturation", 0.25)
        }

    result = run_incrementality_test(df, **params)

    print("Incrementality Test Results:")
    print(result)

    # Optional: save output
    pd.Series(result).to_json("incrementality_test_output.json", indent=2)
