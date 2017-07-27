# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import csv
import sqlite3
from sqlalchemy import Column, Integer, String,  create_engine,Sequence
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

'''
convert CSV<-->Sqlite
'''
class CsvSqlite:

    @property
    def connection(self):
        return self._conn
    
    '''
    :param    fmt:    dictinary.passed to csv.reader/writer as format parameters
    '''
    def __init__(self,db_file=":memory:",enc_csv="cp932",fmt={}):
        self._db_file=db_file
        self._enc_csv = enc_csv
        self._conn = sqlite3.connect(self._db_file)
        self._conn.text_factory = str  # allows utf-8 data to be stored
        self._fmt = fmt

    def __del__(self):
        self._conn.close()

    def csv2sqlite(self,csv_file,tablename):
        c = self._conn.cursor()

        with open(csv_file, "r",encoding=self._enc_csv) as f:
            reader = csv.reader(f,**self._fmt)

            header = True
            for row in reader:
                if header:
                    #process header
                    header = False
                    
                    #create table
                    sql = "DROP TABLE IF EXISTS %s" % tablename
                    c.execute(sql)
                    sql = "CREATE TABLE %s (%s)" % (tablename,
                              ", ".join([ "%s text" % column for column in row ]))
                    c.execute(sql)

                    #create index to the columns which names are *_id
                    for column in row:
                        if column.lower().endswith("_id"):
                            index = "%s__%s" % ( tablename, column )
                            sql = "CREATE INDEX %s on %s (%s)" % ( index, tablename, column )
                            c.execute(sql)

                    insertsql = "INSERT INTO %s VALUES (%s)" % (tablename,
                                ", ".join([ "?" for column in row ]))

                    rowlen = len(row)
                else:
                    # insert row
                    if len(row) == rowlen:
                        c.execute(insertsql, row)

            self._conn.commit()

        c.close()

    def sqlite2csv(self,csv_file,tablename):
        csr=self._conn.execute('SELECT * FROM %(tablename)s' % locals())
        with open(csv_file, "w",encoding=self._enc_csv) as f:
            writer = csv.writer(f,lineterminator='\n',**self._fmt)
            #output header
            row = list(map(lambda cols:cols[0],csr.description))
            writer.writerow(row)

            #output row
            for row in csr:
                writer.writerow(row)
        csr.close()

'''
convert CSV<-->SqlAlchemy
'''
class CsvSqla:

    '''
    :param    fmt:    dictinary.passed to csv.reader/writer as format parameters
    '''
    def __init__(self,db_file="sqlite:///:memory:",enc_csv="cp932",p_echo=False,fmt={}):
        self._engine = create_engine(db_file, echo=p_echo)
        self._Session = sessionmaker(bind=self._engine) #Sessionクラス
        self._enc_csv = enc_csv
        self._fmt = fmt

    def __del__(self):
        pass

    def get_session(self):
        return self._Session()

    '''
    convert CSV-->SqlAlchemy
    
    create sqlite table from csv header
    insert csv datas into table
    returns sqlalchemy class to access to the table
    sqlalchemy needs primary key,so we add auto-incriment id column
    '''

    def csv2sqla(self,csv_file,tablename):
        with open(csv_file, "r",encoding=self._enc_csv) as f:
            reader = csv.reader(f,**self._fmt)

            header = True
            session = self._Session()
            for row in reader:
                if header:
                    #create sqlite table from csv header
                    header = False
                    
                    #define class which name is table name
                    Base = declarative_base()   #base class
                    #sqlalchemy needs primary key,so we add auto-incriment id column
                    attrs = {
                        '__tablename__':tablename,
                        'id':Column( Integer, Sequence(tablename+'_id_seq'), primary_key=True)
                        }
                    for col in row:
                        attrs[col]=Column(String)
                    Model = type(tablename,(Base,),attrs)   #define class 
                    Model.metadata.create_all(self._engine) #create table
                    
                    rowlen = len(row)
                else:
                    #insert csv datas into table
                    if len(row) == rowlen:
                        e = Model()
                        colnames=self.get_col_names(Model.metadata,tablename)
                        for idx in range(0,len(row)):
                            colname = colnames[idx+1]
                            e.__dict__[colname]=row[idx]    #set value of object member,using name of the member as key
                            
                        session.add(e)
                
            session.commit()
        return Model    #returns sqlalchemy class to access to the table

    '''
    convert CSV<--SqlAlchemy
    
    Model    SqlAlchemy class
    '''
    def sqla2csv(self,Model,csv_file):
        
        with open(csv_file, "w",encoding=self._enc_csv) as f:
            writer = csv.writer(f,lineterminator='\n',**self._fmt)
            session = self._Session()
            #output header
            tablename = Model.__name__  #class name
            colnames= self.get_col_names(Model.metadata,tablename)
            writer.writerow(colnames[1:])   #skip first column=id
            
            #output rows
            for row in session.query(Model).all():
                data=[]
                for idx in range(1,len(colnames)):
                    attr = colnames[idx]
                    val = row.__dict__[attr]    #get value of object member,using name of the member as key
                    data.append(val)
                writer.writerow(data)
    
    '''
    get column names of table
    '''
    def get_col_names(self,metadata,tablename):
        return [ col.name for col in metadata.tables[tablename].columns]
    

