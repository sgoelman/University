
import os
import datetime
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

____________________________________________

## Validate that the database file exists otherwise break
if  not os.path.isfile("resturants.csv"):
    print("please add resturants.csv to :"+os.getcwd())

# ____________________________________________

#question (1) loading the database

sc = SparkContext("local", "Simple App")
csv = sc.textFile("resturants.csv")
csv_header = csv.first()
print("Column names:",format(csv_header))
csv_data = csv.filter(lambda line : line != csv_header) #remove csv headers from data
print("Number of rows loaded: {}".format(csv.count()))

# ____________________________________________

class FiveHeaders:
    def __init__(self):
        self.values = []
        self.csv_header = dict()
    def add_headers(self, key, val):
        self.csv_header[key] = val
    def get_header(self, key):
        return self.csv_header[key]
    def get_all_values(self):
        self.values = list(self.csv_header.values())
        return self.values
    def get_index_from_key(self, key):
        index_in_header = self.get_header(key)
        self.values.index(index_in_header)
        return self.values.index(index_in_header)


split_headers= csv_header.split(",")
fiveHeaders = FiveHeaders()
headers = ['business_id', 'business_name', 'business_latitude', 'inspection_date', 'inspection_score']
for header in headers:
     fiveHeaders.add_headers(header, split_headers.index(header))


# ____________________________________________



#problem number (2):

#RDD is assumed to contain an (unique) index column at position 0
def get_RDD_row_by_index(rdd, index=0):
    nrows = rdd.count()
    if index < nrows:
        return rdd.filter(lambda kv: kv[0] >= index)
    return None

#RDD is assumed to contain the requested col_nama
def get_RDD_col_values(rdd, col_name):
    fv = FiveHeaders()
    col_index = fv.get_index_from_key(col_name)
    return rdd.map(lambda line: line[1][col_index])



baseData = csv_data.zipWithIndex().map(lambda line: (line[-1],line[:-1][0].split(",")))
#unique_id
# print("Added unique id at column 0: value: {}".format(baseData.first()[1][6]))
print("take the 75th row from Data: {}".format(get_RDD_row_by_index(baseData,75).first()))





# ____________________________________________

#problem number (3.a): unique values of the 5 columns
#RDD is assumed to contain the requested col_name
def get_RDD_col_values(rdd, col_name):
    fv = FiveHeaders()
    col_index = fv.get_index_from_key(col_name)
    return rdd.map(lambda line: line[1][col_index])


print(get_RDD_col_values(baseData,'business_name'))

# ____________________________________________

import math


class Data_Filter():
    def __init__(self, header_name):
        self.lines = header_name.get_all_values()
        self.build_line = self._builder(self.list_creator)
        self.build_filter = self._builder(self.create_filter_is_num)

    def _builder(self, func):
        return func(self.lines)

    def number(self, s):
        ret = None
        try:
            ret = int(s)
        except ValueError:
            ret = float(s)
        finally:
            return ret

    def is_not_number(self, s):
        ret = False
        try:
            if (math.isnan(self.number(s))):  # not a number
                ret = True
        except:
            ret = True
        finally:
            return ret

    def list_creator(self, lines):
        def get_unique_line(_line, unique_id, append_num=True):
            items = []
            for line in lines:
                if append_num:
                    item = self.number(_line[line])
                else:
                    item = _line[line]
                items.append(item)
            return (unique_id, items)

        return get_unique_line

    def create_filter_is_num(self, lines):

        def check_is_number(_line):
            ret = True
            for line in lines:
                try:
                    if (math.isnan(self.number(_line[line]))):
                        ret = False
                except:
                    print('create_filter_is_num caught an exception ')
                    ret = False
            return ret

        return check_is_number


filtered_data = Data_Filter(fiveHeaders)

# ____________________________________________


filteredByColumn = baseData.filter(lambda line: filtered_data.build_line(line[1])).map(lambda line: filtered_data.build_line(line[1], line[0]))

def count_distinct(filter_by_column, headerName, key):
    return filter_by_column.map(lambda pair: pair[1][headerName.get_index_from_key(key)]).distinct().count()
print(count_distinct(filteredByColumn, fiveHeaders, 'Business_id'))
# for header in headers
#     print(distinctAndCount(filteredByColumn,header,header))
# 'business_id','business_name','business_latitude','inspection_date','inspection_score'
#todo: fix exception


# ____________________________________________