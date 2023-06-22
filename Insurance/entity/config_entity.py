import os, sys
from datetime import datetime
from Insurance.logger import logging
from Insurance.exception import InsuranceException

FEATURE_STORE_FILE_NAME = "Insurance.csv"
TRAIN_FILE_NAME = "train_data.csv"
TEST_FILE_NAME = "test_data.csv"


class TrainingPipelineConfig:

    def __init__(self):
        try:
            self.artifact_dir = os.path.join(os.getcwd(),'artifact',f"{datetime.now().strftime('%d%m%Y_%H%M%S')}")

        except Exception as e:
            raise InsuranceException (e,sys)


class DataIngestionConfig:

    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        try:
            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir,"data_ingestion")
            self.feature_store_file_path = os.path.join(self.data_ingestion_dir,'feature_store',FEATURE_STORE_FILE_NAME)
            self.train_data_file_path = os.path.join(self.data_ingestion_dir,'dataset', TRAIN_FILE_NAME)
            self.test_data_file_path = os.path.join(self.data_ingestion_dir,'dataset', TEST_FILE_NAME)
            self.test_size = 0.2
            self.random_state = 42
        
        except Exception as e:
            raise InsuranceException (e,sys)
        

    def to_dict(self)-> dict:
        try:
            return self.__dict__
        
        except Exception as e:
            raise InsuranceException (e,sys)
        

class DataValidationConfig:
    pass

class DataTransformationConfig:
    pass

class ModelTrainerConfig:
    pass