
class HQLTable():

    def __init__(self, table_name, columns, sub_tables= None, alias = None):
        self.__table_name = table_name
        self.__columns = columns
        self.__sub_tables = sub_tables
        self.__alias = alias

    def table_name(self):
        return self.__table_name

    def table_alias(self):
        return self.__alias

    def table_columns(self):
        return self.__columns

    def sub_tables(self):
        return self.__sub_tables
