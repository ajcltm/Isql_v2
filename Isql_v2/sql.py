from datetime import datetime
import pickle as pickle

def stringfy(value:any)->str:
    if type(value) == str:
        value = value.replace("'", "\\'")
        value = value.replace('"', '\\"')
        return f"'{value}'"

    elif type(value)==datetime:
        value = value.strftime(format='%Y-%m-%d')
        return f"'{value}'"

    elif value == None:
        return "Null"

    else:
        return f'{value}'

class CreateSql:

    def __init__(self, model):
        self.table_name = model.__name__
        self.fields = list(model.__fields__.keys())

    def get_create(self, **customType):
        types = list(customType.values())
        type_part = ', '.join([f'{field} {type_}' for field, type_ in zip(self.fields, types)])
        sql = f'CREATE TABLE {self.table_name} ({type_part})'
        return sql


class InsertSql:

    def __init__(self, model):
        self.table_name = model.__name__
        self.fields = list(model.__fields__.keys())

    def get_values_part(self, data):
        values = data.dict().values()
        values_part_lst = [stringfy(value) for value in values]
        values_part = ', '.join(values_part_lst)
        return f'({values_part})'

    def get_values_parts(self, dataset):
        values_part_lst = [self.get_values_part(i) for i in dataset]
        values_part = ', '.join(values_part_lst)
        return values_part

    def get_insert(self, data):
        values_part = self.get_values_part(data)
        fields_part = ', '.join(self.fields)
        sql = f'INSERT INTO {self.table_name} ({fields_part}) VALUES{values_part}'
        print(sql)
        return sql

    def get_dump(self, dataset):
        values_parts = self.get_values_parts(dataset)
        fields_part = ', '.join(self.fields)
        sql = f'INSERT INTO {self.table_name} ({fields_part}) VALUES{values_parts}'
        print(sql)
        return sql