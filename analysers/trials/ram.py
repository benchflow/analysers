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
containerID = sys.argv[4]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "environment_data"
destTable = "trial_ram"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Ram analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

def computeMetrics(data):
    dataMin = data[-1]
    dataMax = data[0]
    dataLength = len(data)
    median = np.percentile(data, 50).item()
    q1 = np.percentile(data, 25).item()
    q2 = median
    q3 = np.percentile(data, 75).item()
    p95 = np.percentile(data, 95).item()
    mean = np.mean(data, dtype=np.float64).item()
    variance = np.var(data, dtype=np.float64).item()
    stdD = np.std(data, dtype=np.float64).item()
    stdE = stdD/float(math.sqrt(dataLength))
    marginError = stdE * 2
    CILow = mean - marginError
    CIHigh = mean + marginError
    dataIntegral = integrate.trapz(data).item()
    
    return [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "ram_median":median, \
          "ram_mean":mean, "ram_avg":mean, "ram_integral":dataIntegral, "ram_num_data_points":dataLength, \
          "ram_min":dataMin, "ram_max":dataMax, "ram_sd":stdD, \
          "ram_q1":q1, "ram_q2":q2, "ram_q3":q3, "ram_p95":p95, "ram_me":marginError, "ram_ci095_min":CILow, "ram_ci095_max":CIHigh}]

def f(r):
    if r['memory_usage'] == None:
        return (0, 1)
    else:
        return (r['memory_usage'], 1)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("memory_usage") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
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
 
query = computeMetrics(data)
query[0]["ram_mode"] = mode
query[0]["ram_mode_freq"] = highestCount
sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))

print(data[0])