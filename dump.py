from typing import Protocol, Dict, List, Tuple
import os
from pathlib import Path
import pickle
from tqdm import tqdm


class RawDatasetReader:

    def __init__(self, folder_path):
        self.folder_path = folder_path

    def get_key_from_fileName(self, fileName:str) -> str|Tuple:
        fileName_withoutExtention = fileName.split('.')[0]
        key_list = fileName_withoutExtention.split('_')
        if len(key_list) > 1:
            return tuple(key_list)
        return key_list[0]

    def open_file_and_get_rawData(self, fileName:str)-> Dict:
        file_path = self.folder_path.joinpath(fileName)
        with open(file_path, mode='rb') as fr:
            data = pickle.load(fr)
        key = self.get_key_from_fileName(fileName)
        yield {key: data}

    def get_rawDataset(self, file_list) -> List[Dict]:
        return (self.open_file_and_get_rawData(file) for file in tqdm(file_list))

# dataset filter
class IdatasetFilter(Protocol):

    def filtDataset():
        ...


class CompositeDatasetFilter:

    def __init__(self):
        self.filters = []

    def add(self, filtedDataset:IdatasetFilter):
        self.container.append(filtedDataset)
    
    def filtDataset(self, rawDataset):
        currentDataset = rawDataset
        for filter in self.filters:
            currentDataset = filter.filtDataset(currentDataset)
        return currentDataset


class Idumper(Protocol):

    def dump_data():
        ...


class DumpPipeline:

    def __init__(self, rawDataset:List[Dict], datasetFilter: IdatasetFilter|List[IdatasetFilter], dumper:Idumper, file_list:List[str]):
        self.rawDataset = rawDataset
        self.datasetFilter = datasetFilter
        self.dumper = dumper
        self.file_list = file_list

    def get_filtedDataset(self, rawDataset:List[Dict]):
        
        if isinstance(self.datasetFilter, list):
            cf = CompositeDatasetFilter()
            for filter in self.datasetFilter:
                cf.add(filter)
            filtedDataset = cf.filtDataset(rawDataset)
        else:
            filtedDataset = self.datasetFilter.filtDataset(rawDataset)
        return filtedDataset

    def execute(self, commit:bool):
        rawDataset = self.rawDataset.get_rawDataset(self.file_list)
        filtedDataset = self.get_filtedDataset(rawDataset)
        self.dumper.dump_data(filtedDataset, commit)



class DumpApp:

    def __init__(self, folder_path:Path, filtedDataset:IdatasetFilter|List[IdatasetFilter], dumper:Idumper):
        self.folder_path = folder_path
        self.r = RawDatasetReader(self.folder_path)
        self.f = filtedDataset
        self.d = dumper
        
    def chunk_list(list):
        total_num = len(list)
        c, r = divmod(total_num, (total_num/10_000)*2)  # c is always 5000
        return (list[i:i+c] for i in range(0, len(list), c)) 

    def execute(self, commit=True):

        file_list = os.listdir(self.folder_path)
        if len(file_list) > 10000:
            self.chunked_file_list = self.chunk_list(file_list, 5000)
            for file_list in self.chunked_file_list:
                i = DumpPipeline(self.r, self.p, self.d, file_list)
                i.execute(commit)
        else:
            i = DumpPipeline(rawDataset=self.r, datasetFilter=self.f, dumper=self.d, file_list=file_list)
            i.execute(commit)
