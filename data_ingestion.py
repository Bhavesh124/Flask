import shutil
from travelling.entity.config_entity import DataIngestionConfig
import sys,os
from travelling.exception import TravelException
from travelling.logger import logging
from travelling.entity.artifact_entity import DataIngestionArtifact
import tarfile
import numpy as np
import urllib
import urllib.request
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise TravelException(e,sys)
    

    def download_travel_data(self,) -> str:
        try:
            #extraction remote url to download dataset
            download_url = self.data_ingestion_config.dataset_download_url

            #folder location to download file
            tgz_download_dir = self.data_ingestion_config.tgz_download_dir
            
            os.makedirs(tgz_download_dir,exist_ok=True)

            travel_file_name = os.path.basename(download_url)

            tgz_file_path = os.path.join(tgz_download_dir, travel_file_name)

            logging.info(f"Downloading file from :[{download_url}] into :[{tgz_file_path}]")
            urllib.request.urlretrieve(download_url,tgz_file_path)
            logging.info(f"File :[{tgz_file_path}] has been downloaded successfully.")
            return tgz_file_path

        except Exception as e:
            raise TravelException(e,sys) from e
    def extract_tgz_file(self,tgz_file_path:str):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)

            os.makedirs(raw_data_dir,exist_ok=True)
            file_name=os.path.basename(tgz_file_path).split()[0]

            logging.info(f"Extracting tgz file: [{tgz_file_path}] into dir: [{raw_data_dir}]")
            #with tarfile.open(tgz_file_path) as banking_tgz_file_obj:
                #banking_tgz_file_obj.extractall(path=raw_data_dir)
            shutil.copy(tgz_file_path,os.path.join(raw_data_dir,file_name))
            logging.info(f"Extraction completed")

        except Exception as e:
            raise TravelException(e,sys) from e
    
    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            file_name = os.listdir(raw_data_dir)[0]

            travel_file_path = os.path.join(raw_data_dir,file_name)



            logging.info(f"Reading csv file: [{travel_file_path}]")
            travel_data_frame = pd.read_csv(travel_file_path)

          

            logging.info(f"Splitting data into train and test")
            start_train_set = None
            start_test_set = None

            split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

            for train_index,test_index in split.split(travel_data_frame,travel_data_frame["Occupation"]):
                start_train_set = travel_data_frame.loc[train_index].drop(["CustomerID"],axis=1)   #The column names where given wrong, I corrected it
                start_test_set = travel_data_frame.loc[test_index].drop(["CustomerID"],axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)
            
            if start_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting training datset to file: [{train_file_path}]")
                start_train_set.to_csv(train_file_path,index=False)

            if start_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok= True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                start_test_set.to_csv(test_file_path,index=False)
            

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                )
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact

        except Exception as e:
            raise TravelException(e,sys) from e

    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            tgz_file_path =  self.download_travel_data()
            self.extract_tgz_file(tgz_file_path=tgz_file_path)
            return self.split_data_as_train_test()
        except Exception as e:
            raise TravelException(e,sys) from e

