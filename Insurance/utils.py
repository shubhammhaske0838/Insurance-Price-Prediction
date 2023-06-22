import pandas as pd
import numpy as np
import os, sys
from Insurance.logger import logging
from Insurance.exception import InsuranceException
import yaml


def write_yaml_file(filepath:str, data:dict):

    try:
        file_dir = os.path.dirname(filepath)
        os.makedirs(file_dir, exist_ok = True)

        with open(filepath, 'w') as file_write:
            yaml.dump(data, file_write)

    except Exception as e:
        raise InsuranceException(e,sys)


def convert_columns_float(df:pd.DataFrame,exclude_columns:list)->pd.DataFrame:

    try:
        for column in df.columns:
            if column not in exclude_columns:
                if df[column].dtype != 'O':
                    df[column] = df[column].astype('float')
        return df
    
    except Exception as e:
        raise InsuranceException(e,sys)