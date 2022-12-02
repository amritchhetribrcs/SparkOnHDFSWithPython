#Author: Amrit Chhetri, RCS
#Purpose: Reading from HDFS file in cluster node
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
#sc = SparkContext(master="spark://192.168.3.153:7077", appName="WordCount")
sc= SparkContext(master="local", appName="DataOnHDFSApp")
text_file = sc.textFile("hdfs://192.168.3.164:8020/inputdata/Data.txt")
#text_file = sc.textFile("C:\\Data.txt")
countValue = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print("Computed:",countValue)
#countValue.saveAsTextFile("hdfs://192.168.3.164:8020/inputdata/SparkResult6")
#countValue.saveAsTextFile("Results")
sc.stop()
