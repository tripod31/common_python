# coding:utf-8
'''
Created on 2014/12/18

@author: yoshi
'''
import os
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
