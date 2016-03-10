import sys
import json
import io
import gzip
import uuid
import math

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
srcTable = "trial_number_of_process_instances"
destTable = "exp_number_of_process_instances"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Number of process instances analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

def f(r):
    if r['number_of_process_instances'] == None:
        return (0, 1)
    else:
        return (r['number_of_process_instances'], 1)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("number_of_process_instances") \
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
 
#mode = data[0]
dataMin = data[-1]
dataMax = data[0]

#def computeMedian(d):
#    leng = len(d)
#    if leng %2 == 1:
#        medianIndex = ((leng+1)/2)-1
#        return (d[medianIndex], medianIndex, "odd")
#    else:
#        medianIndex = len(d)/2
#        lower = d[leng/2-1]
#        upper = d[leng/2]
#        return ((float(lower + upper)) / 2.0, medianIndex, "even")

dataLength = len(data)
#m = computeMedian(data)
#median = m[0]
#if m[2] == "odd":
#    q3 = computeMedian(data[0:m[1]])[0]
#else:
#    q3 = computeMedian(data[0:m[1]-1])[0]
#q2 = m[0]
#if m[2] == "even":
#    q1 = computeMedian(data[m[1]:])[0]
#else:
#    q1 = computeMedian(data[m[1]+1:])[0]
median = np.percentile(data, 50).item()
q1 = np.percentile(data, 25).item()
q2 = median
q3 = np.percentile(data, 75).item()
p95 = np.percentile(data, 95).item()
      
#mean = reduce(lambda x, y: x + y, data) / float(dataLength)
mean = np.mean(data).item()
#variance = map(lambda x: (x - mean)**2, data)
variance = np.var(data).item()
#stdD = math.sqrt(sum(variance) * 1.0 / dataLength)
stdD = np.std(data).item()

stdE = stdD/float(math.sqrt(dataLength))
marginError = stdE * 2
CILow = mean - marginError
CIHigh = mean + marginError

# TODO: Fix this
query = [{"experiment_id":experimentID, "number_of_process_instances_mode":mode, "number_of_process_instances_mode_freq":highestCount, "number_of_process_instances_median":median, \
          "number_of_process_instances_avg":mean, "number_of_process_instances_num_data_points":dataLength, \
          "number_of_process_instances_min":dataMin, "number_of_process_instances_max":dataMax, "number_of_process_instances_sd":stdD, \
          "number_of_process_instances_q1":q1, "number_of_process_instances_q2":q2, "number_of_process_instances_q3":q3, "number_of_process_instances_p95":p95, \
          "number_of_process_instances_me":marginError, "number_of_process_instances_ci095_min":CILow, "number_of_process_instances_ci095_max":CIHigh}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data[0])