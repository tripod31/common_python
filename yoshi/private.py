# encoding: utf-8
import shutil
import os
from yoshi.util import copy_dir

class MakeZip:
    """
    copy files to temp dir and make zip file from it.
    """
    @property
    def errs(self):
        return self.__errs
    @errs.setter
    def errs(self,value):
        self.__errs = value

    def __init__(self):
        self.__errs = []

    def __get_entry_type(self,entry):
        """
        :param entry:entry of sources to copied
        :return:type
        """
        type = ""
        if isinstance(entry,str):
            type = "simple"
        elif isinstance(entry,dict):
            if set(entry.keys())==set(["src","dst"]):
                type = "src/dst"
            if set(entry.keys())==set(["src","dst","ignore"]):
                type = "src/dst/ignore"
        else:
            type = "unknown"
        return type

    def make_zip(self,app_name,sources,leave_tmp):
        """
        make temp dir.copy distoribute files to temp dir.make zip file from temp dir.
        :param app_name:prifix of zip filename.
        :param sources:list of entries that defines how to copy files/dirs.formats of entry are:
            "src_path"
            {"src":"src path","dst":"dist path"}
            {"src":"src path","dst":"dist path","ignore":"ignore-pattern"}
            ignore-pattern is pattern in wild card style,spllited by comma,like '*.pyc,*tmp'.
        :param leave_tmp:if true,temp dir is not deleted.
        :return:list of error messages
        """
        zip_file=app_name+".zip"
        if os.path.exists(zip_file):
            os.remove(zip_file)

        #make temp dir for making zip
        TMP_DIR="temp"
        if os.path.exists(TMP_DIR):
            shutil.rmtree(TMP_DIR)
        os.mkdir(TMP_DIR)

        self.errs.clear()
        #copy files/dirs
        for ent in sources:
            type = self.__get_entry_type(ent)
            if type == "simple":
                if not os.path.exists(ent):
                    self.errs.append("not exists:"+ent)
                    continue
                if os.path.isfile(ent):
                    shutil.copy(ent,TMP_DIR)
                if os.path.isdir(ent):
                    ent.replace('/',os.path.sep)
                    copy_dir(ent,TMP_DIR)            
            elif type == "src/dst":
                #{"src":"src path","dst":"dist path"}
                if not os.path.exists(ent["src"]):
                    self.errs.append("not exists:"+ent["src"])
                    continue
                if os.path.isfile(ent["src"]):
                    shutil.copy(ent["src"],os.path.join(TMP_DIR,ent["dst"]))
                if os.path.isdir(ent["src"]):
                    shutil.copytree(ent["src"],os.path.join(TMP_DIR,ent["dst"]))
            elif type == "src/dst/ignore":
                #{"src":"src path","dst":"dist path","ignore":"ignore-pattern"}
                if os.path.isdir(ent["src"]):
                    ignore = ent["ignore"].split(',')
                    shutil.copytree(ent["src"],
                        os.path.join(TMP_DIR,ent["dst"]),
                        ignore=shutil.ignore_patterns(*ignore)
                        )
                else:
                    self.errs.append("{0}:src must be directory".format(str(ent)))
            else:
                self.errs.append("wrong format:{0}".format(str(ent)))

        if len(self.errs)==0:
            shutil.make_archive(app_name,"zip",TMP_DIR)
        if (not leave_tmp):
            shutil.rmtree(TMP_DIR)

        return self.errs