from Insurance.logger import logging
from Insurance.exception import InsuranceException
import sys, os
from Insurance.entity.config_entity import DataIngestionConfig, DataValidationConfig
from Insurance.entity import config_entity
from Insurance.components.data_ingestion import DataIngestion
from Insurance.components.data_validation import DataValidation







if __name__ == '__main__':
    try:
        training_pipeline_config = config_entity.TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        print(data_ingestion_config.to_dict())

        data_ingestion_obj = DataIngestion(data_ingestion_config = data_ingestion_config)
        data_ingestion_artifacts = data_ingestion_obj.initiate_data_ingestion()

        data_validation_config = DataValidationConfig(training_pipeline_config=training_pipeline_config)
        data_validation_obj = DataValidation(data_validation_config=data_validation_config, 
                                             data_ingestion_artifact=data_ingestion_artifacts)
        
        data_validation_artifacts = data_validation_obj.initiate_data_validation()


    except Exception as e:
        raise InsuranceException(e,sys)