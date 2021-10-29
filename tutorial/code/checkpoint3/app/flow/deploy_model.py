import luigi
import logging
from pycarol.pipeline import inherit_list
from pycarol.pipeline.targets import PickleTarget
from pycarol import Storage, Carol
from .commons import Task
from . import train_model, evaluate_model

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)

# Adding the dependencies. This tasks will
# be blocked until the Ingestion outputs 
# are available.
#-----------------------------------------
@inherit_list(
    train_model.TrainModel,
    evaluate_model.EvaluateModel
)
class DeployModel(Task):
    deploy_app = luigi.Parameter()
    target_type = PickleTarget

    def easy_run(self, inputs):
        mlp_model, (y_train, y_pred_train), (y_test, y_pred_test) = inputs[0]
        deploy_criteria_flag = inputs[1]

        if deploy_criteria_flag:
            logger.info(f'Deploying the new model to \"{self.deploy_app}\". Note: target app must be under the same tenant.')

            login = Carol()
            login.app_name = self.deploy_app
            stg = Storage(login)
            stg.save("bhp_mlp_model.pkl", mlp_model, format='pickle')

        else:
            logger.warn(f'Model didn\'t achieve the deployment requirements.')

        return