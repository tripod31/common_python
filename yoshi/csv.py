# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import csv
import sqlite3

'''
CSV<-->Sqliteの変換
'''
class CsvSqlite:

    @property
    def connection(self):
        return self._conn

    def __init__(self,db_file=":memory:",enc_csv="cp932"):
        self._db_file=db_file
        self._enc_csv = enc_csv
        self._conn = sqlite3.connect(self._db_file)
        self._conn.text_factory = str  # allows utf-8 data to be stored

    def __del__(self):
        self._conn.close()

    def csv2sqlite(self,csv_file,tablename):
        c = self._conn.cursor()

        # remove the path and extension and use what's left as a table name
        #tablename = os.path.splitext(os.path.basename(csv_file))[0]

        with open(csv_file, "r",encoding=self._enc_csv) as f:
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

            self._conn.commit()

        c.close()

    def sqlite2csv(self,csv_file,tablename):
        csr=self._conn.execute('SELECT * FROM %(tablename)s' % locals())
        with open(csv_file, "w",encoding=self._enc_csv) as f:
            writer = csv.writer(f,lineterminator='\n')
            #ヘッダー出力
            row = list(map(lambda cols:cols[0],csr.description))
            writer.writerow(row)

            for row in csr:
                writer.writerow(row)
        csr.close()

