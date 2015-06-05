# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import os
import csv
import sqlite3
import re
from operator import xor
import zipfile
import fnmatch
import subprocess

'''
配列の共通する要素の配列を求める
'''
def get_common_list(list1,list2):
    set1 = set(list1)
    set2 = set(list2)
    return list(set1 & set2)

def exec_command(cmd,encoding,env=None):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,env=env)
    p.wait()
    stdout_data, stderr_data = p.communicate()
    return p.returncode,\
        stdout_data.decode(encoding).replace("\r\n","\n"),\
        stderr_data.decode(encoding).replace("\r\n","\n"),

'''
    文字列のリストから正規表現の配列に一致するものだけ取り出す
'''
def filter_arr(arr_str,arr_regex,revert=False):
        return filter(lambda s:xor(is_match_patterns(s, arr_regex),revert),arr_str)

'''
    文字列が正規表現の配列に一致するか
'''
def is_match_patterns(s,arr_pattern):
    bRet = False
    for pattern in arr_pattern:
        if re.search(pattern, s) is not None:
            bRet=True
            break
    return bRet

'''
    文字列がfnmatch文字列の配列に一致するか
'''
def is_match_patterns_fnmatch(s,arr_pattern):
    bRet = False
    for pattern in arr_pattern:
        if fnmatch.fnmatch(s, pattern):
            bRet=True
            break
    return bRet

'''
ディレクトリ下のファイルを再帰的に返す
'''
def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)

'''
配列のpathのファイルZIPファイルに出力
filter_func
    pathを引数としてとり、出力するかどうかを返す
'''
def zip_files(start_dir,zip_file,remove_dir=False,filter_func=None):
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        if filter_func is not None:
            arr_path = filter(filter_func,find_all_files(start_dir))
        else:
            arr_path = find_all_files(start_dir)
        for path in arr_path:
            if (remove_dir):
                path_in_zip = os.path.basename(path)
            else:
                path_in_zip = path
            zf.write(path,path_in_zip)

'''
ファイルの文字コードを判定
'''
def get_encoding(path):
    encodings = ('ascii','utf-8','shift_jis','cp932','euc-jp')  #誤認識するのでutf-8を優先する
    data=None
    bSuccess = False
    for encoding in encodings:
        try:
            f = open(path,"r",encoding=encoding)
            data = f.read()
            bSuccess=True
            break
        except:
            pass
        finally:
            f.close()
    if not bSuccess:
        raise Exception("can't decode:%s" % (path))
    return encoding

'''
ファイルの文字コードを変換
'''
def conv_encoding(path,enc_to):
    try:
        enc_org = get_encoding(path)
        f = open(path,"rU",encoding=enc_org)
        data = f.read()
        f.close()
        f = open(path, 'w',encoding=enc_to)
        f.write(data)
        f.close()
        #print("converted:%s->%s\t%s"%(enc_org,enc_to,path))
    except Exception as e:
        raise Exception (path,':',e)
    finally:
        f.close()



'''
CSV<-->Sqliteの変換
'''
class CsvSqlite:
    _db_file = ""
    _enc_csv = ""
    _conn = None

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


'''
python2用
unicode文字列出力時エラー回避
'''
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
'''