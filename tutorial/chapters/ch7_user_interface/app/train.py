import logging
import numpy as np
from pycarol import Carol, CarolHandler, Staging, Storage
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor


def run():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    carol = CarolHandler(Carol())
    carol.setLevel(logging.INFO)
    logger.addHandler(carol)

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
    ]
    y_col = ["target"]
    roi_cols = X_cols + y_col

    data = staging.fetch_parquet(
        staging_name=stag, connector_name=conn, cds=True, columns=roi_cols
    )

    X_train, X_test, y_train, y_test = train_test_split(
        data[X_cols], data[y_col], test_size=0.20, random_state=1
    )

    mlp_model = MLPRegressor(random_state=1, max_iter=500).fit(
        X_train, y_train["target"].values
    )

    y_pred = mlp_model.predict(X_test)

    y_real = list(y_test["target"].values)
    residual = list(y_test["target"].values) - y_pred

    mse_f = np.mean(residual ** 2)
    mae_f = np.mean(abs(residual))
    rmse_f = np.sqrt(mse_f)

    logger.info(f"Mean Squared Error (MSE): {mse_f}.")
    logger.info(f"Mean Absolute Error (MAE): {mae_f}.")
    logger.info(f"Root Mean Squared Error (RMSE): {rmse_f}.")

    stg = Storage(login)
    stg.save("bhp_mlp_regressor", mlp_model, format="pickle")


if __name__ == "__main__":
    run()
