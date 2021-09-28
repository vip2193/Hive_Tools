import os
import sys

from openpyxl import load_workbook


import re


def change_excel_value(file_path):
    if (os.path.isdir(file_path)):
        listdir = os.listdir(file_path)
        for file in listdir:
            workbook = load_workbook(file_path + '/' + file)
            tablename = re.findall(r'-(\w+)V1.0.xlsx',file,re.S|re.I)
            sheet2 = workbook[tablename]
            sheet2['G2'].value = '关联关系'



if __name__ == '__main__':
    # 拿命令行参数
    file_path = str(sys.argv[1])
    change_excel_value(file_path)