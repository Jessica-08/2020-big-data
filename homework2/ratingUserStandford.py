import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

business_path = 'business.csv'
review_path = 'review.csv'
output_path = 'standford_Q3'
app_name = 'Rating of user in Standford'
master = 'local'

sparkConfig = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=sparkConfig)

business_data = sc.textFile(business_path).distinct().map(lambda x: x.split('::')).map(lambda x: (x[0], x[1]))
reviews_data = sc.textFile(review_path).distinct().map(lambda x: x.split('::')).map(lambda x: (str(x[2]), (str(x[1]), float(x[3]))))
standford_data = business_data.filter(lambda x: 'Stanford' in x[1])
result = standford_data.join(reviews_data).map(lambda x: str(x[1][1][0]) + "\t" + str(x[1][1][1]))
# for line in result.collect():
#     print(line)
result.saveAsTextFile(output_path)
