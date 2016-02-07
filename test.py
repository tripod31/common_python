#/usr/bin/env python3
# coding:utf-8

import unittest
import os

from yoshi.util import is_match_patterns,filter_arr,zip_files,find_all_files
from yoshi.csv import CsvSqlite,CsvSqla

def create_csv():
    if not os.path.exists('test'):
        os.mkdir('test')
    
    f=open("test/test_in_sjis.csv","w",encoding="cp932")
    f.write("name,age\n")
    f.write("abe,51\n")
    f.close()
    
class Test_util(unittest.TestCase):
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
        arr_path=list(filter_arr(find_all_files("test"),["\.csv$"]));
        print(arr_path)
    def test5(self):
        zip_files("test", "test/zipwpath.zip",
                  filter_func=lambda path:is_match_patterns(path, ["\.csv$"])
                )
    
class Test_csvsqlite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_csv()
        
    def setUp(self):
        pass
    
    def test_csv_sqlite(self):
        #読み込み
        obj = CsvSqlite()
        obj.csv2sqlite("test/test_in_sjis.csv","table1")
        csr = obj.connection.execute('select * from table1 where name="abe"')
        row = csr.fetchone()
        self.assertEqual(row[0],"abe")
        self.assertEqual(row[1],"51")
        
        #変更
        obj.connection.execute('update table1 set age=52 where name="abe"')
        obj.connection.commit()
        
        #書き込み             
        obj.sqlite2csv("test/test_out_sjis.csv","table1")
    
        #再読み込み
        obj = CsvSqlite()
        obj.csv2sqlite("test/test_out_sjis.csv","table1")
        csr = obj.connection.execute('select * from table1 where name="abe"')
        row = csr.fetchone()
        self.assertEqual(row[0],"abe")
        self.assertEqual(row[1],"52")
    
    
    def tearDown(self):
        pass

class Test_csvsqla(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_csv_sqla(self):

        #読み込み
        obj = CsvSqla()
        Model=obj.csv2sqla("test/test_in_sjis.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.name,"abe")
        self.assertEqual(row.age,"51")
        
        #変更
        row.age = 52
        session.commit()          
        
        #書き込み
        obj.sqla2csv(Model,"test/test_out_sjis.csv",)
        
        #再度読み込み
        obj = CsvSqla()
        Model=obj.csv2sqla("test/test_out_sjis.csv","table1")
        session = obj.get_session()
        row = session.query(Model).first()
        self.assertEqual(row.name,"abe")
        self.assertEqual(row.age,"52")        
        
    def tearDown(self):
        pass
        
if __name__ == '__main__':
    #unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test_csvsqlite('test_csv_sqlite'))
    suite.addTest(Test_csvsqla('test_csv_sqla'))
    unittest.TextTestRunner(verbosity=2).run(suite)
