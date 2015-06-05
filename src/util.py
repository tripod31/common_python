# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import os
import csv
import sqlite3

def csv2sqlite(csv_file,db_file,tablename):
    conn = sqlite3.connect(db_file)
    conn.text_factory = str  # allows utf-8 data to be stored
    
    c = conn.cursor()
     
    # remove the path and extension and use what's left as a table name
    #tablename = os.path.splitext(os.path.basename(csv_file))[0]
 
    with open(csv_file, "r") as f:
        reader = csv.reader(f)
 
        header = True
        for row in reader:
            if header:
                # gather column names from the first row of the csv
                header = False
 
                sql = "DROP TABLE IF EXISTS %s" % tablename
                c.execute(sql)
                sql = "CREATE TABLE %s (%s)" % (tablename,
                          ", ".join([ "%s text" % column for column in row ]))
                c.execute(sql)
 
                for column in row:
                    if column.lower().endswith("_id"):
                        index = "%s__%s" % ( tablename, column )
                        sql = "CREATE INDEX %s on %s (%s)" % ( index, tablename, column )
                        c.execute(sql)
 
                insertsql = "INSERT INTO %s VALUES (%s)" % (tablename,
                            ", ".join([ "?" for column in row ]))
 
                rowlen = len(row)
            else:
                # skip lines that don't have the right number of columns
                if len(row) == rowlen:
                    c.execute(insertsql, row)
 
        conn.commit()
     
    c.close()
    conn.close()

def sqlite2csv(csv_file,db_file,tablename):
    conn = sqlite3.connect(db_file)
    conn.text_factory = str  # allows utf-8 data to be stored
    
    csr=conn.execute('SELECT * FROM %(tablename)s' % locals())
    with open(csv_file, "w") as f:
        writer = csv.writer(f,lineterminator='\n')
        #ヘッダー出力
        row=[]
        for col in csr.description:
            col_name = col[0]
            row.append(col_name)
        writer.writerow(row)
        
        for row in csr:
            writer.writerow(row)
    csr.close()
    conn.close()

def fild_all_files(directory):
    for root, dirs, files in os.walk(directory):
        #yield root
        for file in files:
            #if os.path.isfile(file):
            yield os.path.join(root, file)

'''
python2用
unicode文字列出力時エラー回避
'''
class MyDictWriter(csv.DictWriter):
    out_encoding=""
    
    def __init__(self, f, fieldnames, restval="", extrasaction="raise", dialect="excel", out_encoding="utf-8",*args, **kwds):
        self.out_encoding=out_encoding
        csv.DictWriter.__init__(self, f, fieldnames, restval=restval, extrasaction=extrasaction, dialect=dialect, *args, **kwds)

    def writerows(self, rowdicts):
        for row in rowdicts:
            #unicode文字列をcsv.DictWriterに渡すとエラーになるのでエンコード
            row_new=dict()
            for (key,val) in row.items():
                row_new[key]=val.encode(self.out_encoding)
            csv.DictWriter.writerow(self, row_new)