# coding:utf-8
import util
import unittest

class MyTest(unittest.TestCase):
    def setUp(self):
        pass
    def test1(self):
        util.csv2sqlite("test/test_in.csv","test/test.db","table1")
        util.sqlite2csv("test/test_out.csv","test/test.db","table1")
    def test2(self):
        util.csv2sqlite("test/test_in_sjis.csv","test/test.db","table1")
        util.sqlite2csv("test/test_out_sjis.csv","test/test.db","table1")
    def tearDown(self): 
        pass
    
if __name__ == '__main__':
    unittest.main()
