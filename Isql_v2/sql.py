from datetime import datetime, date
import pickle as pickle

def stringfy(value:any, dbtype)->str:
    if type(value) == str:
        if dbtype == 'mysql':
            symbols = ['\\', "'", '"', "(", ")", "%", '&', '@', '*', '[', ']', '{', '}', '^', '!', '/', '-', '+', '?', ';', '~', '|']
            for symbol in symbols:
                value = value.replace(symbol, '\\'+f'{symbol}') 
            return f"'{value}'"
        else :
            symbols = ['\\', '"', "(", ")", "%", '&', '@', '*', '[', ']', '{', '}', '^', '!', '/', '-', '+', '?', ';', '~', '|']
            for symbol in symbols:
                value = value.replace(symbol, '\\'+f'{symbol}') 
            value = value.replace("'", "'"+f"'") 
            return f"'{value}'"

    elif type(value)==datetime or type(value)==date:
        return f"'{value}'"

    elif value == None:
        return 'Null'

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
        print(f'create sql : \n {sql}')
        return sql


class InsertSql:

    def __init__(self, model):
        self.table_name = model.__name__
        self.fields = list(model.__fields__.keys())

    def get_values_part(self, data, dbtype):
        values = data.dict().values()
        values_part_lst = [stringfy(value=value, dbtype=dbtype) for value in values]
        values_part = ', '.join(values_part_lst)
        return f'({values_part})'

    def get_values_parts(self, dataset, dbtype):
        values_part_lst = [self.get_values_part(data=i, dbtype=dbtype) for i in dataset]
        values_part = ', '.join(values_part_lst)
        return values_part

    def get_insert(self, data, dbtype='mysql'):
        values_part = self.get_values_part(data=data, dbtype=dbtype)
        fields_part = ', '.join(self.fields)
        sql = f'INSERT INTO {self.table_name} ({fields_part}) VALUES{values_part}'
        print(f'insert sql : \n {sql}')
        return sql

    def get_dump(self, dataset, dbtype='mysql'):
        values_parts = self.get_values_parts(dataset=dataset, dbtype=dbtype)
        fields_part = ', '.join(self.fields)
        sql = f'INSERT INTO {self.table_name} ({fields_part}) VALUES{values_parts}'
        print(f'dump sql : \n {sql[:250]}')
        return sql
