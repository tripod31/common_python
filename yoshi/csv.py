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
from builtins import isinstance
import io

class CsvSqlite:
    '''
    convert CSV <--> Sqlite
    '''

    @property
    def connection(self):
        return self._conn
    
    def __init__(self,db_file=":memory:",enc_csv="utf-8",header=True,fmt={}):
        '''
        :param    enc_csv:csv encoding
        :param    header:When true,It asume that first line of csv is header.When false,column names are 'col0','col1'...
        :param    fmt:dictinary.passed to csv.reader/writer as format parameters
        '''
        self._db_file=db_file
        self._enc_csv = enc_csv
        self._conn = sqlite3.connect(self._db_file)
        self._conn.text_factory = str           # allows utf-8 data to be stored
        self._conn.row_factory = sqlite3.Row    #to enable access by column name,like row['name']
        self._header = header
        self._fmt = fmt

    def __del__(self):
        self._conn.close()

    def csv2sqlite(self,csv_file,tablename):
        '''
        Convert CSV --> Sqlite
        Create sqlite table from csv header.
        All the other columns are text columns.
        Create index to the columns which names are '*_id'.
        insert csv datas into table.
        :param    csv_file:str of file name,or file object
        :param    tablename:name of the table that csv datas are imported to
        '''
        #f:file object to read csv file        
        def proc(f):
            c = self._conn.cursor()
            reader = csv.reader(f,**self._fmt)

            header = True
            for row in reader:
                if header:
                    #process header
                    header = False
                    
                    #drop previous table
                    sql = "DROP TABLE IF EXISTS %s" % tablename
                    c.execute(sql)
                    insertsql = "INSERT INTO %s VALUES (%s)" % (tablename,",".join(["?"]*len(row) ))
                    colnum = len(row)
                                        
                    if self._header:
                        #create table
                        sql = "CREATE TABLE %s (%s)" % (tablename,",".join([ "%s TEXT" % column for column in row ]))
                        c.execute(sql)
    
                        #create index to the columns which names are '*_id'
                        for column in row:
                            if column.lower().endswith("_id"):
                                index = "%s__%s" % ( tablename, column )
                                sql = "CREATE INDEX %s on %s (%s)" % ( index, tablename, column )
                                c.execute(sql)
                    else:
                        #create table
                        #names of columns are 'COL0','COL1',...
                        sql = "CREATE TABLE %s (%s)" % (tablename,",".join([ "col%d" % i for i in range(0,len(row)) ]))
                        c.execute(sql)
                        
                        c.execute(insertsql, row)
                else:
                    # insert row
                    if len(row) == colnum:
                        c.execute(insertsql, row)

            self._conn.commit()
            c.close()
        
        #main
        if isinstance(csv_file,str):
            with open(csv_file, "r",encoding=self._enc_csv) as f:
                proc(f)
        elif isinstance(csv_file,io.IOBase):
            proc(csv_file)
        else:
            raise ValueError("parameter 'csv_file' is'nt string or file object")
    
    def sqlite2csv(self,csv_file,tablename):
        '''
        :param    csv_file:str of file name,or file object
        :param    tablename:name of the table that csv datas are exported from
        '''
        #f:file object to read csv file
        def proc(f):
            csr=self._conn.execute('SELECT * FROM %s' % tablename)
            writer = csv.writer(f,lineterminator='\n',**self._fmt)
            
            if self._header:
                #output header
                colnames = list(map(lambda cols:cols[0],csr.description))
                writer.writerow(colnames)

            #output row
            for row in csr:
                writer.writerow(row)
            csr.close()
        
        #main
        if isinstance(csv_file,str):
            with open(csv_file, "w",encoding=self._enc_csv) as f:
                proc(f)
        elif isinstance(csv_file,io.IOBase):
            proc(csv_file)
        else:
            raise ValueError("parameter 'csv_file' is'nt string or file object")
                    
class CsvSqla:
    '''
    convert CSV <--> SqlAlchemy
    '''   

    def __init__(self,db_file="sqlite:///:memory:",enc_csv="utf-8",header=True,p_echo=False,fmt={}):
        '''
        :param    enc_csv:csv encoding
        :param    header:When true,It asume that first line of csv is header
        :param    fmt:dictinary.passed to csv.reader/writer as format parameters
        '''
        self._engine = create_engine(db_file, echo=p_echo)
        self._Session = sessionmaker(bind=self._engine) #Sessionクラス
        self._enc_csv = enc_csv
        self._header = header
        self._fmt = fmt

    def __del__(self):
        pass

    def get_session(self):
        return self._Session()

    def csv2sqla(self,csv_file,tablename):
        '''
        convert CSV-->SqlAlchemy
        Create sqlite table from csv header.
        All columns are text columns.
        SqlAlchemy needs primary key,so we add auto-incriment "_id_" column.    
        insert csv datas into table.
        return: SqlAlchemy class to access to the table.
        '''
        #f:file object to read csv file        
        def proc(f):
            reader = csv.reader(f,**self._fmt)

            header = True
            session = self._Session()
            for row in reader:
                if header:
                    header = False
                    
                    #create sqlite table from csv header                    
                    #define class dynamically,which name is table name
                    Base = declarative_base()   #base class
                    #sqlalchemy needs primary key,so we add auto-incriment id column
                    attrs = {
                        '__tablename__':tablename,
                        '_id_':Column( Integer, Sequence(tablename+'_id_seq'), primary_key=True)
                    }
                    if self._header:
                        for col in row:
                            attrs[col]=Column(String)           #all column as string
                    else:
                        i=0
                        for col in row:
                            attrs["col%d" % i]=Column(String)   #all column as string                        
                            i = i+1
                    
                    Model = type(tablename,(Base,),attrs)   #define class 
                    Model.metadata.create_all(self._engine) #create table
                    
                    colnum = len(row)
                    if not self._header:
                        self.insert_row(Model, tablename, row,session)
                else:
                    #insert csv datas into table
                    if len(row) == colnum:
                        self.insert_row(Model, tablename, row,session)
                
            session.commit()
            return Model
        
        #main
        if isinstance(csv_file,str):
            with open(csv_file, "r",encoding=self._enc_csv) as f:
                Model=proc(f)
        elif isinstance(csv_file,io.IOBase):
            Model=proc(csv_file)
        else:
            raise ValueError("parameter 'csv_file' is'nt string or file object")
        
        return Model    #returns sqlalchemy class to access to the table

    def insert_row(self,Model,tablename,row,session):
        e = Model()
        colnames=self.get_col_names(Model.metadata,tablename)
        for idx in range(0,len(row)):
            colname = colnames[idx+1]
            e.__dict__[colname]=row[idx]    #set value of object member,using name of the member as key
            
        session.add(e)
        
    def sqla2csv(self,Model,csv_file):
        '''
        convert CSV<--SqlAlchemy
        :param Model:SqlAlchemy class
        '''
        #f:file object to read csv file        
        def proc(f):
            writer = csv.writer(f,lineterminator='\n',**self._fmt)
            session = self._Session()
            tablename = Model.__name__  #class name
            colnames= self.get_col_names(Model.metadata,tablename)
            
            if self._header:
                #output header
                writer.writerow(colnames[1:])   #skip first column=id
            
            #output rows
            for row in session.query(Model).all():
                data=[]
                for idx in range(1,len(colnames)):
                    attr = colnames[idx]
                    val = row.__dict__[attr]    #get value of object member,using name of the member as key
                    data.append(val)
                writer.writerow(data)
        
        #main
        if isinstance(csv_file,str):
            with open(csv_file, "w",encoding=self._enc_csv) as f:
                proc(f)
        elif isinstance(csv_file,io.IOBase):
            proc(csv_file)
        else:
            raise ValueError("parameter 'csv_file' is'nt string or file object")
            
    def get_col_names(self,metadata,tablename):
        '''
        get column names of table
        '''
        return [ col.name for col in metadata.tables[tablename].columns]
