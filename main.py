from Insurance.logger import logging
from Insurance.exception import InsuranceException
import sys, os
from Insurance.entity.config_entity import DataIngestionConfig
from Insurance.entity import config_entity
from Insurance.components.data_ingestion import DataIngestion







if __name__ == '__main__':
    try:
        training_pipeline_config = config_entity.TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        print(data_ingestion_config.to_dict())

        data_ingestion_obj = DataIngestion(data_ingestion_config = data_ingestion_config)
        data_ingestion_obj.initiate_data_ingestion()

    except Exception as e:
        raise InsuranceException(e,sys)