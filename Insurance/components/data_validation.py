import pandas as pd
import numpy as np
import os, sys
from Insurance.logger import logging
from Insurance.exception import InsuranceException
from Insurance.entity.artifact_entity import DataValidationArtifact, DataIngestionArtifact
from Insurance.entity.config_entity import DataValidationConfig
from typing import Optional
from scipy.stats import ks_2samp
from Insurance.utils import convert_columns_float, write_yaml_file

class DataValidation:
    def __init__(self,data_validation_config:DataValidationConfig,
                 data_ingestion_artifact:DataIngestionArtifact):
                
        try:
            logging.info(f"*****************Data Validation*****************")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error = dict()

        except Exception as e:
            raise InsuranceException(e,sys)
        
    def drop_missing_values_columns(self, df:pd.DataFrame, report_key_name:str)->Optional[pd.DataFrame]:
        try:
            threshold = self.data_validation_config.missing_threshold
            null_report = df.isna().sum()/df.shape[0]
            drop_columns_names = null_report[null_report > threshold].index

            self.validation_error[report_key_name] = list(drop_columns_names)

            df.drop(list(drop_columns_names), axis=1, inplace = True)

            if len(df.columns)==0:
                return None
            
            return df

        except Exception as e:
            raise InsuranceException(e,sys)
        
    def is_required_columns_exists(self, based_df:pd.DataFrame, current_df:pd.DataFrame, report_key_name:str):
        
        try:
            ###########################################################################################
            base_columns = based_df
            current_columns = current_df

            missing_columns = []

            for base_column in  base_columns:
                if base_column not in current_columns:
                    logging.info(f"Column:[{base_column} is not available]")
                    missing_columns.append(base_column)

            if len(missing_columns)>0:
                self.validation_error[report_key_name] = missing_columns
                return False
            
            return True

        except Exception as e:
            raise InsuranceException (e,sys)
        
    def data_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, report_key_name:str):
        try:
            drift_report = dict()

            base_columns = base_df.columns
            current_columns = current_df.columns

            for column in base_columns:
                base_data, current_data = base_df[column], current_df[column]
                same_distribution = ks_2samp(base_data, current_data)

                if same_distribution.pvalue > 0.05:
                    drift_report[column] = {
                        "p-values":float(same_distribution.pvalue),
                        "same_distribution": True
                    }
                
                else:
                    drift_report[column] = {
                        "p-values":float(same_distribution.pvalue),
                        "same_distribution": False
                    }

            self.validation_error[report_key_name] = drift_report


        except Exception as e:
            raise InsuranceException(e,sys)
        
        
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace(to_replace ='na', value = np.NAN, inplace = True)
            base_df = self.drop_missing_values_columns(df=base_df, report_key_name='missing_values_within_base_dataset')
            
            train_df = pd.read_csv(self.data_ingestion_artifact.train_data_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_data_file_path)

            train_df = self.drop_missing_values_columns(df=train_df, report_key_name='missing_values_within_current_train_dataset')
            test_df = self.drop_missing_values_columns(df=test_df, report_key_name='missing_values_within_current_test_dataset')

            exclude_column = [self.data_validation_config.target_column]
            base_df = convert_columns_float(df = base_df, exclude_columns=exclude_column)
            train_df = convert_columns_float(df = train_df, exclude_columns=exclude_column)
            test_df = convert_columns_float(df = test_df, exclude_columns=exclude_column)

            train_df_columns_status = self.is_required_columns_exists(based_df=base_df, current_df=train_df, report_key_name='missing_columns_train_dataset')
            test_df_columns_status = self.is_required_columns_exists(based_df=base_df, current_df=test_df, report_key_name='missing_columns_test_dataset')

            if train_df_columns_status:
                self.data_drift(base_df=base_df, current_df= train_df, report_key_name='data_drift_within_train_dataset')
                
            if test_df_columns_status:
                self.data_drift(base_df=base_df, current_df= test_df, report_key_name='data_drift_within_test_dataset')

            # write your report

            write_yaml_file(self.data_validation_config.report_file_path, data = self.validation_error)

            data_validation_artifact = DataValidationArtifact(
                report_file_path=self.data_validation_config.report_file_path
            )

            return data_validation_artifact

        except Exception as e:
            raise InsuranceException(e,sys)


























