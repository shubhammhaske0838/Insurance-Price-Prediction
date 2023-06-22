import pandas as pd
import numpy as np
import os, sys
from Insurance.entity import config_entity
from Insurance.entity import artifact_entity
from Insurance.exception import InsuranceException
from Insurance.logger import logging
from sklearn.model_selection import train_test_split


class DataIngestion:
    def __init__(self,data_ingestion_config:config_entity.DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise InsuranceException(e,sys)
        
    def initiate_data_ingestion(self)->artifact_entity.DataIngestionArtifact:

        try:

            logging.info("Read the data from csv file")
            df = pd.read_csv(r"insurance.csv")

            logging.info(f"dataset columns:[{df.columns}]")
            logging.info(f'Rows and Columns: [{df.shape}]')

            df.replace(to_replace ='na', value = np.NAN, inplace = True)

            logging.info("Create feature store folder if not availabel")

            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir, exist_ok = True)

            logging.info("save df to feature store folder")
            df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path, index= False, header=True)

            logging.info('Splitting our data into the train and test set')
            train_df, test_df = train_test_split(df, test_size = self.data_ingestion_config.test_size, 
                                                random_state= self.data_ingestion_config.random_state)
            
            logging.info("Create dataset directory folder if not available")
            dataset_dir = os.path.dirname(self.data_ingestion_config.train_data_file_path)
            os.makedirs(dataset_dir, exist_ok = True)

            logging.info("Save train and test dataset into the dataset folder")
            train_df.to_csv(self.data_ingestion_config.train_data_file_path, index= False, header = True)
            test_df.to_csv(self.data_ingestion_config.test_data_file_path, index= False, header = True)

            #prepare artifact folder

            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                feature_store_file_path = self.data_ingestion_config.feature_store_file_path,
                train_data_file_path = self.data_ingestion_config.train_data_file_path,
                test_data_file_path = self.data_ingestion_config.test_data_file_path
            )

            return data_ingestion_artifact
        
        except Exception as e:
            raise InsuranceException(e,sys)



        



        






