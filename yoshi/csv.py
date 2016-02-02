# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import csv
import sqlite3
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine,Sequence
from sqlalchemy.orm import mapper,sessionmaker
import types

'''
CSV<-->SqlAlchemyの変換
CSVのヘッダからsqliteのテーブルを作成
テーブルにCSVのデータを入れる
そのテーブルを操作するためのSqlAlchemyのクラスを返す
'''
class CsvSqla:

    def __init__(self,db_file="sqlite:///:memory:",enc_csv="cp932",p_echo=True):
        self._engine = create_engine(db_file, echo=p_echo)
        self._Session = sessionmaker(bind=self._engine) #Sessionクラス
        self._metadata = MetaData()
        self._enc_csv = enc_csv

    def __del__(self):
        pass

    def get_session(self):
        return self._Session()

    '''
    CSV-->SqlAlchemyの変換

    CSVのヘッダからsqliteのテーブルを作成
    　テーブルにCSVのデータを入れる
    　そのテーブルを操作するためのSqlAlchemyのクラスを返す
    '''

    def csv2sqla(self,csv_file,tablename):
        with open(csv_file, "r",encoding=self._enc_csv) as f:
            reader = csv.reader(f)

            header = True
            session = self._Session()
            for row in reader:
                if header:
                    # CSVのヘッダからsqlalchemyのクラス、sqliteのテーブルを作成
                    header = False
                    
                    cols = [ Column('id', Integer, Sequence(tablename+'_id_seq'), primary_key=True)]    #primary key is mandetary
                    for col in row:
                        cols.append(Column(col,String))
                    table = Table(tablename,self._metadata,*cols)
                    
                    self._metadata.create_all(self._engine)
                    Model = types.new_class(tablename)    #define class
                    mapper(Model,table)
                    
                    rowlen = len(row)
                else:
                    # テーブルにCSVのデータを入れる
                    if len(row) == rowlen:
                        e = Model()
                        colnames=self.get_col_names(tablename)
                        for idx in range(0,len(row)):
                            colname = colnames[idx+1]
                            e.__dict__[colname]=row[idx]    #set attribute of object
                            
                        session.add(e)
                
            session.commit()
        return Model    #テーブルを操作するためのSqlAlchemyのクラスを返す

    '''
    CSV<--SqlAlchemyの変換
    
    Model    SqlAlchemyのクラス
    '''
    def sqla2csv(self,Model,csv_file):
        
        with open(csv_file, "w",encoding=self._enc_csv) as f:
            writer = csv.writer(f,lineterminator='\n')
            session = self._Session()
            #ヘッダー出力
            tablename = Model.__name__  #class name
            colnames= self.get_col_names(tablename)   
            writer.writerow(colnames[1:])   #skip first column=id
            
            #行出力
            for row in session.query(Model).all():
                data=[]
                for idx in range(1,len(colnames)):
                    attr = colnames[idx]
                    val = row.__dict__[attr]    #get attribute of Model object
                    data.append(val)
                writer.writerow(data)
    
    def get_col_names(self,tablename):
        return [ col.name for col in self._metadata.tables[tablename].columns]
    
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

