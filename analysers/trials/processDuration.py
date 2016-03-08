import sys
import json
import io
import gzip
import uuid
import math

import numpy as np

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
experimentID = trialID.split("_")[0]
nToIgnore = 5
cassandraKeyspace = "benchflow"
srcTable = "process"
destTable = "trial_process_duration"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Process duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("process_definition_id", "source_process_instance_id", "start_time", "duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .filter(lambda r: r["process_definition_id"] is not None) \
        .cache()

processes = dataRDD.map(lambda r: r["process_definition_id"]) \
        .distinct() \
        .collect()

maxTime = None
maxID = None
for p in processes:
    time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
        .map(lambda r: (r["start_time"], r["source_process_instance_id"])) \
        .sortByKey(1, 1) \
        .collect()
    if len(time) < nToIgnore:
        continue
    else:
        time = time[nToIgnore-1]
    if maxTime is None or time[0] > maxTime:
        maxTime = time[0]
        defId = dataRDD.filter(lambda r: r["process_definition_id"] == p and r["start_time"] == maxTime) \
            .map(lambda r: (r["source_process_instance_id"], 0)) \
            .sortByKey(1, 1) \
            .first()
        maxID = defId[0]

print(maxTime)

data = dataRDD.map(lambda r: (r["start_time"], r)) \
        .sortByKey(1, 1) \
        .map(lambda r: r[1]) \
        .collect()

index = -1
if maxID is not None:
    for i in range(len(data)):
        if data[i]["source_process_instance_id"] == maxID:
            index = i
            break

data = sc.parallelize(data[index+1:])

def f(r):
    if r['duration'] == None:
        return (0, 1)
    else:
        return (r['duration'], 1)

dataRDD = data.map(f) \
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
mean = np.mean(data, dtype=np.float64).item()
#variance = map(lambda x: (x - mean)**2, data)
variance = np.var(data, dtype=np.float64).item()
#stdD = math.sqrt(sum(variance) * 1.0 / dataLength)
stdD = np.std(data, dtype=np.float64).item()

stdE = stdD/float(math.sqrt(dataLength))
marginError = stdE * 2
CILow = mean - marginError
CIHigh = mean + marginError

# TODO: Fix this
query = [{"experiment_id":experimentID, "trial_id":trialID, "process_duration_mode":mode, "process_duration_median":median, \
          "process_duration_mean":mean, "process_duration_avg":mean, "process_duration_num_data_points":dataLength, \
          "process_duration_min":dataMin, "process_duration_max":dataMax, "process_duration_sd":stdD, \
          "process_duration_q1":q1, "process_duration_q2":q2, "process_duration_q3":q3, "process_duration_p95":p95, \
          "process_duration_me":marginError, "process_duration_ci095_min":CILow, "process_duration_ci095_max":CIHigh}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data[0])