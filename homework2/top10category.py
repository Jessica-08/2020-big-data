import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

business_path = 'business.csv'
review_path = 'review.csv'
output_path = 'top10category_Q6'
app_name = 'TOP 10 CATEGORY'
master = 'local'

sparkConfig = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=sparkConfig)

business_data = sc.textFile(business_path).distinct().map(lambda x: (x.split("::")[0],x.split("::")))
group_split_data = business_data.flatMap(lambda x: [s.strip() for s in x[1][2][5:-1].split(",")]).map(lambda x: (x, 1))
count_data = group_split_data.reduceByKey(lambda x,y:x+y).map(lambda x: x[0] + "\t" + str(x[1]))
temp = count_data.top(10, key=lambda x: int(x.split("\t")[1]))
top10_category_data = sc.parallelize(temp)
top10_category_data.saveAsTextFile(output_path)