import luigi
import logging
from pycarol.pipeline import inherit_list
from pycarol.pipeline.targets import PickleTarget
from pycarol import Storage
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor

# Importing the task with basic configurations
# in place to use as baseline. This task will
# be extend to perform the business logic we
# want.
#-----------------------------------------
from .commons import Task

# This task depends on the ingestion outputs.
# We import the class here to add as a depen
# dency.
#-----------------------------------------
from . import ingestion

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)

# Adding the dependencies. This tasks will
# be blocked until the Ingestion outputs 
# are available.
#-----------------------------------------
@inherit_list(
    ingestion.IngestRecords
)
class TrainModel(Task):

    # Recovering parameters from settings
    #-----------------------------------------
    model_l2regularization = luigi.Parameter()
    model_epochs = luigi.Parameter()

    # Selecting how the output will be persis-
    # ted after the processing.
    #-----------------------------------------
    target_type = PickleTarget

    def easy_run(self, inputs):

        # Retrieving outputs from previous task
        #-----------------------------------------
        data = inputs[0]

        logger.info(f'Spliting train/ test portions of the data.')
        X_cols = ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"] 
        y_col = ["target"]
        X_train, X_test, y_train, y_test = train_test_split(data[X_cols],
                                                            data[y_col], 
                                                            test_size=0.20, 
                                                            random_state=1)

        logger.info(f'Training a MLP with {X_train.shape[0]} samples.')
        mlp_model = MLPRegressor(random_state=1, 
                                 alpha=self.model_l2regularization,
                                 max_iter=self.model_epochs).fit(X_train, y_train["target"].values)

        logger.info(f'Making predictions for {X_test.shape[0]} records on test set.')
        y_pred_train = mlp_model.predict(X_train)
        y_pred_test = mlp_model.predict(X_test)

        # Transforming all to lists
        y_train = list(y_train["target"].values)
        y_test = list(y_test["target"].values)
        y_pred_train = list(y_pred_train["target"].values)
        y_pred_test = list(y_pred_test["target"].values)

        # Returning the model and real/predictions 
        # pairs for train and test (used on validation)
        #-----------------------------------------
        return (mlp_model, (y_train, y_pred_train), (y_test, y_pred_test))