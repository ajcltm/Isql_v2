from Isql_v2 import sql

import unittest
from datetime import datetime
from pydantic import BaseModel

class Test_stringfy(unittest.TestCase):

    def test_string_with_comma(self):

        test_string_1 = "'x'"
        processed_1 = sql.stringfy(test_string_1)
        self.assertEqual("'\\'x\\''", processed_1)  # -> ''\x\''

        test_string_2 = '"y"'
        processed_2 = sql.stringfy(test_string_2)
        answer = '''\\"y\\"''' # -> "\y\"
        self.assertEqual(f"'{answer}'", processed_2) # -> '"\y\"'

    def test_datetime_to_sql_style(self):

        test_date = datetime(1923, 8, 29)
        processed = sql.stringfy(test_date)
        self.assertEqual("'1923-08-29'", processed) # -> '1923-08-29'
        
    def test_none_value(self):
        test_value = None
        processed = sql.stringfy(test_value)
        self.assertEqual("Null", processed) # -> Null

    def test_number_value(self):
        test_value = 1234
        processed = sql.stringfy(test_value)
        self.assertEqual('1234', processed) # -> 1234

    def test_usual_words(self):
        test_word = 'kim'
        processed = sql.stringfy(test_word)
        self.assertEqual("'kim'", processed) # -> 'kim'


class Test_CreateSql(unittest.TestCase):

    def test_get_create(self):

        class Model(BaseModel):
            attr_1: str
            attr_2: int

        return_sql = sql.CreateSql(model=Model).get_create(attr_1='str', attr_2='int')
        self.assertEqual('CREATE TABLE Model (attr_1 str, attr_2 int)', return_sql)

        
class Test_InsertSql(unittest.TestCase):

    def test_get_insert(self):

        class Model(BaseModel):
            attr_1: str
            attr_2: int

        test_data = Model(attr_1='kim', attr_2=1234) 

        return_sql = sql.InsertSql(model=Model).get_insert(data=test_data)
        self.assertEqual("INSERT INTO Model (attr_1, attr_2) VALUES('kim', 1234)", return_sql)


    def test_get_dump(self):
        class Model(BaseModel):
            attr_1: str
            attr_2: int

        test_data_1 = Model(attr_1='kim', attr_2=1234)
        test_data_2 = Model(attr_1='lee', attr_2=5678) 

        return_sql = sql.InsertSql(model=Model).get_dump(dataset=[test_data_1, test_data_2])
        self.assertEqual("INSERT INTO Model (attr_1, attr_2) VALUES('kim', 1234), ('lee', 5678)", return_sql)

if __name__ == '__main__':
    unittest.main()