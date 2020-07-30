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
import tempfile
import shutil
import threading
import time

'''
Functions
=========
Data
'''

def get_common_list(list1,list2):
    """
    get list of common menbers of two lists
    """
    set1 = set(list1)
    set2 = set(list2)
    return list(set1 & set2)

'''
Functions
=========
String
'''

def print_arr(arr,p_format):
    '''
    display strings in two dimension array
    Each column length is adjusted to max length of data
    :param arr:two dimension array of strings,columns(list) x rows(list)
    :param p_format:format of output
    '''
    if len(arr)==0:
        return
    
    row_len=len(arr[0])
    max_len=[0]*row_len

    #get max length of columns
    for row in arr:
        for idx in range(0,row_len):
            if max_len[idx] < len(row[idx]):
                max_len[idx] = len(row[idx])
    #print
    for row in arr:
        for idx in range(0,len(row)):
            col = row[idx]
            col = col + " " * (max_len[idx]-len(col))
            row[idx]=col
        line = p_format % tuple(row)
        print(line)
    
def filter_arr(arr_str,arr_regex,revert=False):
    '''
    filter list of string,which matches regular expression
    '''
    return filter(lambda s:xor(is_match_patterns(s, arr_regex),revert),arr_str)

def is_all_ascii(s):
    '''
    returns whether contains only ascii charactor,or not
    (ascii chars except controll chars)
    '''
    if re.search(r'^[\x20-\x7E]+$',s) is None:
        return False
    else:
        return True

def is_match_patterns(s,arr_pattern):
    '''
    returns whether string matches regular expression or not
    '''
    bRet = False
    for pattern in arr_pattern:
        if re.search(pattern, s) is not None:
            bRet=True
            break
    return bRet

def is_match_patterns_fnmatch(s,arr_pattern):
    '''
    returns whether string matches fnmatch expression or not
    '''
    bRet = False
    for pattern in arr_pattern:
        if fnmatch.fnmatch(s, pattern):
            bRet=True
            break
    return bRet

'''
Functions
=========
File
'''
#used by copy_with_progress()
def disp_progress(src,dst):
    size_src = os.path.getsize(src)
    print_flg = False
    max_len = 0
    while not stop_flg:
        if os.path.exists(dst):
            size_dst =  os.path.getsize(dst)
            msg = "copying {:>4.0f}MB/{:>4.0f}MB {}->{}".format(size_dst/1024**2,size_src/1024**2,src,dst)
            if len(msg)>max_len:
                max_len = len(msg)
            print("\r"+msg,end="")
            print_flg = True
        time.sleep(1)
    if print_flg:
        print("\rdone" + " " * (max_len-4))

def copy_with_progress(src, dst, *, follow_symlinks=True):
    global stop_flg

    ret = ""
    if os.path.getsize(src)>1024**2 :
        #display progress if file size is big
        stop_flg=False
        th = threading.Thread(target = disp_progress,args=(src,dst))
        th.start()
        ex = None
        try: 
           ret = shutil.copy2(src,dst,follow_symlinks=False)
        except Exception as e:
           ex = e

        stop_flg = True
        th.join()
        if ex is not None:
           raise ex
    else:
        ret = shutil.copy2(src,dst,follow_symlinks=False)
    return ret

def copy_dir(src,dst):
    """
    copy dir,in same way as 'cp -r dir1 dir2'
    separater in dir arguments should be os.path.sep.in windows,'\\'.
    :param src:source dir.
    :param dst:dist dir.source dir is copied to dist dir(dir2/dir1 is created).
    """

    dirs = src.split(os.path.sep)
    src_dir = dirs[len(dirs)-1]
    shutil.copytree(src,os.path.join(dst,src_dir))

def find_all_files(directory):
    '''
    returns files under directory
    '''
    for root, _dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)

def zip_files(start_dir,zip_file,remove_dir=False,filter_func=None):
    '''
    put files under directory to zip file
    :param filter_func:take path as argument,returns whether the file is to be output or not
    '''

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

def get_eol(s):
    '''
    returns end of line chars of string
    :return
    '\\r\\n,'\\n','\\r',''(no end of line)
    '''
    if re.search('\r\n',s):
        return '\r\n'
    
    if re.search('\n',s):
        return '\n'
    
    if re.search('\r',s):
        return '\r'
        
    return ''

def get_encoding(path):
    '''
    get encoding of file
    '''

    encodings = ('ascii','utf-8','shift_jis','cp932','euc-jp')  #prevail utf-8,otherwise it fails to recognite encoding
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

class EncodeException(Exception):
    pass

def conv_encoding(path,to_enc,to_eol=None):
    '''
    converts encoding of file
    :param to_eol:end of line which is converted to.
    '\\r\\n'(CRLF),'\\n'(LF).'\\r'(CR)
    '''
    org_enc,data = get_encoding(path)
    eol = get_eol(data)
    
    #convert eol
    if to_eol is not None and eol is not None:
        data = data.replace(eol,to_eol)

    #write data to tempfile    
    byte_arr = data.encode(to_enc)
    temp_file= tempfile.mkstemp()
    ft = os.fdopen(temp_file[0],mode='w+b') #binary mode,to prevent end of line to be changed
    ft.write(byte_arr)
    
    #verify data
    ft.seek(0)
    new_bytes = ft.read()
    ft.close()
    data_new =new_bytes.decode(to_enc)
    
    if data_new != data:
        raise EncodeException('verify data failed')
    
    try:    
        shutil.copyfile(temp_file[1], path)
    except Exception as e:
        print (str(e))
        
    os.remove(temp_file[1])

def replace_str(file,from_str,to_str):
    '''
    replace string in file
    '''
    hit =False
    try:
        enc,data = get_encoding(file)
        if data.find(from_str) != -1:
            hit = True
        
            data = data.replace(from_str, to_str)
            bytes_data = data.encode(enc) 
            temp_file= tempfile.mkstemp()
            ft = os.fdopen(temp_file[0],mode='w+b') #binary mode,to prevent end of line to be changed
            ft.write(bytes_data)
    except Exception as e:
        print(str(e))
        return
    
    if not hit:
        return
    
    #verify data
    ft.seek(0)
    new_bytes = ft.read()
    ft.close()
    data_new =new_bytes.decode(enc)
    
    if data_new != data:
        raise EncodeException('verify data failed')
    try:
        shutil.copyfile(temp_file[1], file)
    except Exception as e:
        print (str(e))
        
    os.remove(temp_file[1])
    
'''
Functions
=========
Process
'''

def exec_command(cmd,encoding,env=None):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,env=env)
    #p.wait()
    stdout_data, stderr_data = p.communicate()
    return p.returncode,\
        stdout_data.decode(encoding).replace("\r\n","\n"),\
        stderr_data.decode(encoding).replace("\r\n","\n"),
