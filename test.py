# coding:utf-8
import unittest
from yoshi.util import is_match_patterns,filter_arr,zip_files,CsvSqlite,find_all_files

class Test1(unittest.TestCase):
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
    
class Test_csv(unittest.TestCase):
    def setUp(self):
        pass
    def test_csv_sqlite(self):
        obj = CsvSqlite()
        obj.csv2sqlite("test/test_in_sjis.csv","table1")
        csr = obj.connection.execute('select * from table1 where name="よし"')
        row = csr.fetchone()
        #print(row)
        self.assertEqual(row[0],"よし")
        self.assertEqual(row[1],"51")             
        obj.sqlite2csv("test/test_out_sjis.csv","table1")
    def tearDown(self):
        pass
    
if __name__ == '__main__':
    unittest.main()
