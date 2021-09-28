# 批量生成
import os
import sys
import time

from tools import Hive_2_Excel

ERR_PATH = 'D:/tmp/excel_error/'

def read_file_to_sql(m_file):
    hql = ''
    with open(m_file, encoding='utf-8') as f:
        hql = f.read()
    return hql

def write_file(content,filename):
    if not os.path.exists(ERR_PATH):
        os.mkdir(ERR_PATH)
    filepath = ERR_PATH  + filename + '.txt'
    with open(filepath, 'a+',encoding='utf-8',newline='') as f:
        f.write(content)
    return filepath

def hive2Excel(m_file):
    if (os.path.isdir(m_file)):
        listdir = os.listdir(m_file)
        for file in listdir:
            SQL = read_file_to_sql(m_file + '/' + file)
            try:
                filepath, filename = Hive_2_Excel.hive_2_excel(SQL)
            except:
                write_file(m_file + '/' + file,'转换失败的SQL' + str(time.time()) + '.txt')
    else:
        read_file_to_sql(m_file)

if __name__ == '__main__':
    # 拿命令行参数
    m_file = str(sys.argv[1])
    if len(sys.argv) > 2:
        TARGET_PATH = str(sys.argv[2])

    hive2Excel(m_file)
