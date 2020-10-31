import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

business_path = 'business.csv'
review_path = 'review.csv'
output_path = 'top10business_Q4'
app_name = 'TOP 10 BUSINESS'
master = 'local'

sparkConfig = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=sparkConfig)

business_data = sc.textFile(business_path).distinct().map(lambda x: (x.split("::")[0],x.split("::")))
#id business_id
reviews_data = sc.textFile(review_path).distinct().map(lambda x: (x.split("::")[2],x.split("::")))
#id business_id
avg_data = reviews_data.map(lambda x: (x[0], (float(x[1][3]), 1.0))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
#business_id avg_rating
join_data = business_data.join(avg_data)
# for line in join_data.collect():
#     print(line)
result_data = join_data.map(lambda x:x[0]+"\t"+x[1][0][1]+"\t"+x[1][0][2]+"\t"+str(x[1][1]))
temp= result_data.top(10,key = lambda x:float(x.split("\t")[-1]))
top10_data = sc.parallelize(temp)
top10_data.saveAsTextFile(output_path)
