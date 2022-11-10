from Isql_v2 import dump, sql
import unittest
from pathlib import Path
import pickle
import os
import shutil
from pydantic import BaseModel
import sqlite3
from mock import Mock
import pickle

class Test_1_RawDatasetReader(unittest.TestCase):

    def setUp(self):
        data_1 = {'key1': 1923, 'key2': 1924}
        data_2 = {'key1': 1988, 'key2': 1989}

        self.folder_path = Path().cwd().joinpath('test', 'test_temp_folder')
        os.makedirs(self.folder_path)

        self.file_path_1 = self.folder_path.joinpath('parentKey_1111.pickle')
        with open(file=self.file_path_1, mode='wb') as fw:
            pickle.dump(data_1, fw)

        self.file_path_2 = self.folder_path.joinpath('parentKey_2222.pickle')
        with open(file=self.file_path_2, mode='wb') as fw:
            pickle.dump(data_2, fw)

    def tearDown(self) :
        shutil.rmtree(self.folder_path)

    def test_get_rawDataset(self):
        file_list = os.listdir(self.folder_path)
        rdr = dump.RawDatasetReader(folder_path=self.folder_path)
        rawDataset = rdr.get_rawDataset(file_list) # ({('parentKey', '1111'): {'key1': 1923, 'key2': 1924}., ('parentKey', '2222'): {'key1': 1988, 'key2': 1989}})
        rawDataset_list_type = list(rawDataset) # [{('parentKey', '1111'): {'key1': 1923, 'key2': 1924}., ('parentKey', '2222'): {'key1': 1988, 'key2': 1989}}]
        self.assertEqual(1923, list(rawDataset_list_type[0].values())[0].get('key1'))
        self.assertEqual(1989, list(rawDataset_list_type[1].values())[0].get('key2'))


class Test_2_DumpPipeline(unittest.TestCase):

    def setUp(self) -> None:
        self.folder_path = Path.cwd().joinpath('test', 'test_db')
        os.makedirs(self.folder_path)
        self.db_path = Path.cwd().joinpath('test', 'test_db', 'testDB.db')
        self.con = sqlite3.connect(self.db_path)
        print(f'Test DB is created !')

    def tearDown(self) -> None:
        shutil.rmtree(self.folder_path)

    def test_execute(self):

        rawDataset = ({('parent_key_1', 'parent_key_2'): {'key_1':1234, 'key_2': 5678}}, {('parent_key_1', 'parent_key_2'): {'key_1':4578, 'key_2': 9876}})
        RawDatasetLoader = Mock()
        RawDatasetLoader.get_rawDataset.return_value = rawDataset


        class DataModel(BaseModel):
            key_1: int
        
        class DatasetFilter:
            def filt(self, dataset):
                filted_dataset=[]
                for data in dataset:
                    print(f'data : {data}')
                    for key, value in data.items():
                        print(f'key: {key}, value: {value}')
                        filted_dataset.append(DataModel(key_1=value.get('key_1')))
                return filted_dataset

        class dumper:

            def __init__(self, con):
                create_sql = sql.CreateSql(model=DataModel).get_create(key_1='text')
                self.con = con
                self.cur = self.con.cursor()
                self.cur.execute(create_sql)
                self.con.commit()

            def dump_data(self, dataset, commit=True):
                insert_sql = sql.InsertSql(model=DataModel).get_dump(dataset=dataset)
                self.cur.execute(insert_sql)
                if commit:
                    self.con.commit()
        
        dpl = dump.DumpPipeline(rawDataset=RawDatasetLoader, datasetFilter=DatasetFilter(), dumper=dumper(con=self.con), file_list=['file_1', 'file_2'])
        dpl.execute(commit=True)

        cur = self.con.cursor()
        cur.execute('select key_1 from DataModel')
        rows = list(cur.fetchall())
        self.assertEqual(1234, int(rows[0][0]))
        self.assertEqual(4578, int(rows[1][0]))

        self.con.close()


class Test_3_DumpApp(unittest.TestCase):

    def setUp(self) -> None:
        self.pickle_folder_path = Path.cwd().joinpath('test', 'test_pickle_folder')
        os.makedirs(self.pickle_folder_path)

        response_json = [{'key_1': 1234, 'key_2': 4567}, {'key_1': 7653, 'key_2': 9876}]
        for data in response_json:
            name_ref = data.get('key_1')
            file_path = self.pickle_folder_path.joinpath(f'parentkey1_{name_ref+473}_parentkey2_{name_ref+823}.pickle')
            with open(file_path, mode='wb') as fw:
                pickle.dump(data, fw)
        file_list = os.listdir(self.pickle_folder_path)
        print(f'file_list : {file_list}')

        self.db_folder_path = Path.cwd().joinpath('test', 'test_db')
        os.makedirs(self.db_folder_path)
        self.db_path = Path.cwd().joinpath('test', 'test_db', 'testDB.db')
        self.con = sqlite3.connect(self.db_path)
        print(f'Test DB is created !')

    def tearDown(self) -> None:
        shutil.rmtree(self.pickle_folder_path)
        shutil.rmtree(self.db_folder_path)
    
    def test_dump_app(self):

        class DataModel(BaseModel):
            key_1: int
        
        class DatasetFilter:
            def filt(self, dataset):
                filted_dataset=[]
                for data in dataset:
                    print(f'data : {data}')
                    for key, value in data.items():
                        print(f'key: {key}, value: {value}')
                        filted_dataset.append(DataModel(key_1=value.get('key_1')))
                return filted_dataset

        class dumper:

            def __init__(self, con):
                create_sql = sql.CreateSql(model=DataModel).get_create(key_1='text')
                self.con = con
                self.cur = self.con.cursor()
                self.cur.execute(create_sql)
                self.con.commit()

            def dump_data(self, dataset, commit=True):
                insert_sql = sql.InsertSql(model=DataModel).get_dump(dataset=dataset)
                self.cur.execute(insert_sql)
                if commit:
                    self.con.commit()

        da = dump.DumpApp(folder_path=self.pickle_folder_path, filtedDataset=DatasetFilter(), dumper=dumper(con=self.con))
        da.execute()

        cur = self.con.cursor()
        cur.execute('select key_1 from DataModel')
        rows = list(cur.fetchall())
        self.assertEqual(1234, int(rows[0][0]))
        self.assertEqual(7653, int(rows[1][0]))

        self.con.close()

if __name__ == '__main__':
    unittest.main()

    # folder_path = Path.cwd().joinpath('test', 'test_db')
    # file_list = ['parentkey1_1707_parentkey2_2057.pickle', 'parentkey1_8126_parentkey2_8476.pickle']
    # for file in file_list:
    #     file_path = folder_path.joinpath(file)
    #     with open(file_path, mode='rb') as fr:
    #         data = pickle.load(fr)
    #     print(data)