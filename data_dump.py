import pymongo
import pandas as pd
import json

client = pymongo.MongoClient("mongodb+srv://shubhammhaske0838:<23febsdm20201>@cluster0.skktwbt.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH = (r"C:\Python\End to End Project\Resume Based Projects\Insurance-Price-Prediction\insurance.csv")
DATABASE_NAME = "INSURANCE"
COLLECTION_NAME = "INSURANCE_DATA"


if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns: {df.shape}")

    df.reset_index(drop = True, inplace = True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)