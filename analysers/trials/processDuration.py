import sys
import json
import io
import gzip
import uuid
import math

import scipy.integrate as integrate
import scipy.special as special

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "process"
destTable = "exp_process_duration"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Process duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

def f(r):
    if r['duration'] == None:
        return (0, 1)
    else:
        return (r['duration'], 1)
    
data = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(f) \
        .reduceByKey(lambda a, b: a + b) \
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

data = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(f) \
        .sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()
 
mode = data[0]
dataMin = data[-1]
dataMax = data[0]

def computeMedian(d):
    leng = len(d)
    if leng %2 == 1:
        medianIndex = ((leng+1)/2)-1
        return (d[medianIndex], medianIndex, "odd")
    else:
        medianIndex = len(d)/2
        lower = d[leng/2-1]
        upper = d[leng/2]
        return ((float(lower + upper)) / 2.0, medianIndex, "even")

dataLength = len(data)
m = computeMedian(data)
median = m[0]
if m[2] == "odd":
    q3 = computeMedian(data[0:m[1]])[0]
else:
    q3 = computeMedian(data[0:m[1]-1])[0]
q2 = m[0]
if m[2] == "even":
    q1 = computeMedian(data[m[1]:])[0]
else:
    q1 = computeMedian(data[m[1]+1:])[0]
      
mean = reduce(lambda x, y: x + y, data) / float(dataLength)
variance = map(lambda x: (x - mean)**2, data)
stdD = math.sqrt(sum(variance) * 1.0 / dataLength)

stdE = stdD/float(math.sqrt(dataLength))
marginError = stdE * 2
CILow = mean - marginError
CIHigh = mean + marginError
CI = [CILow, CIHigh]

dataIntegral = sum(integrate.cumtrapz(data))[0]

# TODO: Fix this
query = [{"experiment_id":experimentID, "trial_id":trialID, "process_duration_mode":mode, "process_duration_median":median, \
          "process_duration_mean":mean, "process_duration_avg":mean, "process_duration_integral":dataIntegral, "process_duration_weight":dataLength, \
          "process_duration_min":dataMin, "process_duration_max":dataMax, "process_duration_sd":stdD, \
          "process_duration_q1":q1, "process_duration_q2":q2, "process_duration_q3":q3, "process_duration_me":marginError, "process_duration_ci095":CI}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data[0])