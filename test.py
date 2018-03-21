#/usr/bin/env python3
# coding:utf-8

import unittest
import os
import shutil

from yoshi.util import is_match_patterns,filter_arr,zip_files,conv_encoding
from yoshi.csv import CsvSqlite,CsvSqla
    
def create_test_dir():
    if os.path.exists('test'):
        shutil.rmtree('test')
    os.mkdir('test')

def create_csv():    
    with open("test/test_in_sjis.csv","w",encoding="cp932") as f:
        f.write("name,age\n")
        f.write("abe,51\n")
        f.close()
    
    with open("test/test_in_sjis2.csv","w",encoding="cp932") as f:
        f.write("abe,51\n")
        f.write("yoshi,32\n")
        f.close()

class Test_util(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_test_dir()

    def test1(self):
        self.assertEqual(is_match_patterns("abcdef",["ab","cd"]),True)
        self.assertEqual(is_match_patterns("abcdef",["gh","ij"]),False)
    def test2(self):
        l= list(filter_arr(["abc","def","ghi"], ["bc"]))
        self.assertListEqual(l, ["abc"])
    def test3(self):
        l= list(filter_arr(["abc","def","ghi"], ["bc"],True))
        self.assertListEqual(l, ["def","ghi"])
    def test4(self):
        create_csv()
        zip_files("test", "test/zipwpath.zip",
                  filter_func=lambda path:is_match_patterns(path, ["\\.csv$"])
                )

class Test_csvsqlite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_test_dir()
        create_csv()
        
    def setUp(self):
        pass
    
    '''
    read csv with header
    '''
    def test_csv_sqlite(self):
        #read csv
        obj = CsvSqlite(enc_csv='cp932')
        obj.csv2sqlite("test/test_in_sjis.csv","table1")
        csr = obj.connection.execute('select * from table1 where name="abe"')
        row = csr.fetchone()
        self.assertEqual(row['name'],"abe")
        self.assertEqual(row['age'],"51")
        
        #modify data
        obj.connection.execute('update table1 set age=52 where name="abe"')
        obj.connection.commit()
        
        #write csv    
        obj.sqlite2csv("test/test_out_sjis.csv","table1")
    
        #read csv again
        obj = CsvSqlite(enc_csv='cp932')
        obj.csv2sqlite("test/test_out_sjis.csv","table1")
        csr = obj.connection.execute('select * from table1 where name="abe"')
        row = csr.fetchone()
        self.assertEqual(row['name'],"abe")
        self.assertEqual(row['age'],"52")
    
    '''
    read csv without header
    '''
    def test_csv_sqlite_2(self):
        #read csv
        obj = CsvSqlite(enc_csv='cp932',header=False)
        obj.csv2sqlite("test/test_in_sjis2.csv","table1")
        csr = obj.connection.execute('select * from table1 where col0="abe"')
        row = csr.fetchone()
        self.assertEqual(row['col0'],"abe")
        self.assertEqual(row['col1'],"51")
        
        #modify data
        obj.connection.execute('update table1 set col1=52 where col0="abe"')
        obj.connection.commit()
        
        #write csv    
        obj.sqlite2csv("test/test_out_sjis2.csv","table1")
    
        #read csv again
        obj = CsvSqlite(enc_csv='cp932',header=False)
        obj.csv2sqlite("test/test_out_sjis2.csv","table1")
        csr = obj.connection.execute('select * from table1 where col0 ="abe"')
        row = csr.fetchone()
        self.assertEqual(row['col0'],"abe")
        self.assertEqual(row['col1'],"52")    
    
    def tearDown(self):
        pass

class Test_csvsqla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_test_dir()
        create_csv()
        
    def setUp(self):
        pass
    
    def test_csv_sqla(self):

        #read csv
        obj = CsvSqla(enc_csv='cp932')
        Model=obj.csv2sqla("test/test_in_sjis.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.name,"abe")
        self.assertEqual(row.age,"51")
        
        #modify data
        row.age = 52
        session.commit()          
        
        #write csv
        obj.sqla2csv(Model,"test/test_out_sjis.csv",)
        
        #read csv again
        obj = CsvSqla()
        Model=obj.csv2sqla("test/test_out_sjis.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.name,"abe")
        self.assertEqual(row.age,"52")        
    
    '''
    read csv without header
    '''
    def test_csv_sqla2(self):

        #read csv
        obj = CsvSqla(enc_csv='cp932',header=False)
        Model=obj.csv2sqla("test/test_in_sjis2.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.col0,"abe")
        self.assertEqual(row.col1,"51")
        
        #modify data
        row.col1 = 52
        session.commit()          
        
        #write csv
        obj.sqla2csv(Model,"test/test_out_sjis2.csv",)
        
        #read csv again
        obj = CsvSqla(enc_csv='cp932',header=False)
        Model=obj.csv2sqla("test/test_out_sjis2.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.col0,"abe")
        self.assertEqual(row.col1,"52")        
        
    def tearDown(self):
        pass


def create_file(s,enc):
    if not os.path.exists('test'):
        os.mkdir('test')
    
    with open("test/test.txt","w",encoding=enc,newline='') as f:
        f.write(s)
        f.close()

class Test_conv_encoding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_test_dir()
        
    def setUp(self):
        pass
    
    def test_encoding(self):
        create_file('あああ\r\n\r\nいいい\r\n','cp932')
        conv_encoding('test/test.txt', 'utf-8', '\n')
        with open("test/test.txt","r",encoding="utf-8",newline='') as f:
            data = f.read()
        self.assertEqual(data,'あああ\n\nいいい\n')
        
if __name__ == '__main__':
    unittest.main()
    '''
    suite = unittest.TestSuite()
    suite.addTest(Test_conv_encoding('test_encoding'))
    unittest.TextTestRunner(verbosity=2).run(suite)
    '''