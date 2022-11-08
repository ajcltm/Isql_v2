from Isql_v2 import dump
import unittest
from pathlib import Path
import pickle
import os
import shutil

class Test_RawDatasetReader(unittest.TestCase):

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

if __name__ == '__main__':
    unittest.main()
        