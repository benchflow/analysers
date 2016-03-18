import sys
import json
import io
import gzip
import uuid
import math

from datetime import timedelta

import scipy.integrate as integrate
import scipy.special as special
import numpy as np

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "trial_byte_size"
destTable = "exp_byte_size"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Database size analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

def f(r):
    if r['size'] == None:
        return (0, 1)
    else:
        return (r['size'], 1)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("size") \
        .where("experiment_id=?", experimentID) \
        .map(f) \
        .cache()

data = dataRDD.reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(0, 1) \
        .collect()
    
mode = list()
highestCount = data[0][0]        
for d in data:
    if d[0] == highestCount:
        mode.append(d[1])
    else:
        break

data = dataRDD.sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()
 
dataMin = data[-1]
dataMax = data[0]
dataLength = len(data)
median = np.percentile(data, 50).item()
q1 = np.percentile(data, 25).item()
q2 = median
q3 = np.percentile(data, 75).item()
p95 = np.percentile(data, 95).item()
mean = np.mean(data).item()
variance = np.var(data).item()
stdD = np.std(data).item()
stdE = stdD/float(math.sqrt(dataLength))
marginError = stdE * 2
CILow = mean - marginError
CIHigh = mean + marginError

# TODO: Fix this
query = [{"experiment_id":experimentID, "size_mode":mode, "size_mode_freq":highestCount, "size_median":median, \
          "size_avg":mean, "size_num_data_points":dataLength, \
          "size_min":dataMin, "size_max":dataMax, "size_sd":stdD, \
          "size_q1":q1, "size_q2":q2, "size_q3":q3, "size_p95":p95, \
          "size_me":marginError, "size_ci095_min":CILow, "size_ci095_max":CIHigh}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))

print(data[0])