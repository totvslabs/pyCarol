import pandas as pd
from pycarol import Carol, Staging, Storage
from sklearn.model_selection import train_test_split


def run():
    login = Carol()
    staging = Staging(login)

    conn = "boston_house_price"
    stag = "samples"

    X_cols = [
        "CRIM",
        "ZN",
        "INDUS",
        "CHAS",
        "NOX",
        "RM",
        "AGE",
        "DIS",
        "RAD",
        "TAX",
        "PTRATIO",
        "B",
        "LSTAT",
        "sample",
    ]
    roi_cols = X_cols

    data = staging.fetch_parquet(
        staging_name=stag, connector_name=conn, cds=True, columns=roi_cols
    )

    _, X_test = train_test_split(data[X_cols], test_size=0.20, random_state=1)

    stg = Storage(login)
    mlp_model = stg.load("bhp_mlp_regressor", format="pickle")

    test_cols = [
        "CRIM",
        "ZN",
        "INDUS",
        "CHAS",
        "NOX",
        "RM",
        "AGE",
        "DIS",
        "RAD",
        "TAX",
        "PTRATIO",
        "B",
        "LSTAT",
    ]
    y_pred = mlp_model.predict(X_test[test_cols])

    predictions = X_test[["sample"]].copy()
    predictions["predicted_value"] = y_pred
    predictions["prediction_date"] = pd.Timestamp.now()

    staging = Staging(login)
    staging.send_data(
        "predictions",
        data=predictions,
        connector_name="model",
    )


if __name__ == "__main__":
    run()
