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
get list of common menbers of two lists
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
filter list of string,which matches regular expression
'''
def filter_arr(arr_str,arr_regex,revert=False):
        return filter(lambda s:xor(is_match_patterns(s, arr_regex),revert),arr_str)

'''
returns whether string matches regular expression or not
'''
def is_match_patterns(s,arr_pattern):
    bRet = False
    for pattern in arr_pattern:
        if re.search(pattern, s) is not None:
            bRet=True
            break
    return bRet

'''
returns whether string matches fnmatch expression or not
'''
def is_match_patterns_fnmatch(s,arr_pattern):
    bRet = False
    for pattern in arr_pattern:
        if fnmatch.fnmatch(s, pattern):
            bRet=True
            break
    return bRet

'''
returns files under directory
'''
def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)

'''
put files under directory to zip file

filter_func
    take path as argument,returns whether the file is to be output or not
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

class DecodeException(Exception):
    pass

'''
get encoding of file
'''
def get_encoding(path):
    encodings = ('ascii','utf-8','shift_jis','cp932','euc-jp')  #prevail utf-8,otherwise it misrecognite encoding
    data=None
    bSuccess = False
    for encoding in encodings:
        try:
            with open(path,"r",encoding=encoding,newline='') as f:   #newline='',does'nt convert end of line
                data = f.read()
                f.close()
                bSuccess=True
            break
        except:
            pass
        finally:
            f.close()
    if not bSuccess:
        raise DecodeException(path)
    return encoding,data

'''
converts encoding of file

to_eol
    end of line which is converted to.
    '\r\n' or '\n'
'''
def conv_encoding(path,to_enc,to_eol=None):
    try:
        org_enc,data = get_encoding(path)
        if to_eol is not None:
            data = re.sub('[\r\n]+',to_eol,data)
        
        with open(path, 'w',encoding=to_enc,newline='') as f:   #newline='',改行コードの変換をしない
            f.write(data)
            f.close()

    except Exception as e:
        raise Exception (path,':',e)
