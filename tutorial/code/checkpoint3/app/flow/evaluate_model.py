from .commons import Task
import luigi
from . import train_model
from pycarol.pipeline import inherit_list
import logging
import numpy as np

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)

@inherit_list(
    train_model.TrainModel
)
class EvaluateModel(Task):

    def easy_run(self, inputs):
        mlp_model, (y_train, y_pred_train), (y_test, y_pred_test) = inputs[0]

        # Here we set the model to be always deployed.
        # We can build better rules to deploy the model
        # only when satisfying basic evaluation thresholds
        deploy_criteria_flag = True

        residual_train = y_train - y_pred_train
        residual_test = y_test - y_pred_test


        for phase, residual in [("Train", residual_train), ("Test", residual_test)]:
            mse_f = np.mean(residual**2)
            mae_f = np.mean(abs(residual))
            rmse_f = np.sqrt(mse_f)

            logger.info(f"{phase} - Mean Squared Error (MSE): {mse_f}.")
            logger.info(f"{phase} - Mean Absolute Error (MAE): {mae_f}.")
            logger.info(f"{phase} - Root Mean Squared Error (RMSE): {rmse_f}.")

        return deploy_criteria_flag
