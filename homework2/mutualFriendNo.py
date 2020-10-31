import pyspark #alt+enter
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

data_path = 'soc-LiveJournal1Adj.txt'
output_path = 'mutual_friend_number_Q1'
app_name = 'Mutual Friend Number'
master = 'local'

sparkConfig = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=sparkConfig)

data = sc.textFile(data_path)

def mutual_friend_mapper(line):
    item = line.split("\t")
    user = item[0]
    friends = item[1].split(",")
    lst = []
    for friend in friends:
        if friend.isnumeric():
            person1 = str(min(int(user), int(friend)))
            person2 = str(max(int(user), int(friend)))
            key = person1 + "," + person2
            value = item[1]
            lst += [(key, (key, value))]
    return lst

def mutual_friend_reducer(l1,l2):
    key1,key2= l1[0],l2[0]
    value1,value2 = l1[1].split(","),l2[1].split(",")
    lst = []
    for e in value1:
        if e in value2:
            lst.append(e)
    return str(len(lst))

question1_out = data.flatMap(mutual_friend_mapper).reduceByKey(mutual_friend_reducer).filter(lambda x: isinstance(x[1], str)).map(lambda x: x[0] + "\t" + x[1])

question1_out.saveAsTextFile(output_path)

