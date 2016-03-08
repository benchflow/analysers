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
containerID = sys.argv[4]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "environment_data"
destTable = "trial_cpu"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Cpu analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations
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
    dataIntegral = sum(integrate.cumtrapz(data)).item()

    # TODO: Fix this
    return [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "cpu_median":median, \
              "cpu_mean":mean, "cpu_avg":mean, "cpu_integral":dataIntegral, "cpu_num_data_points":dataLength, \
              "cpu_min":dataMin, "cpu_max":dataMax, "cpu_sd":stdD, \
              "cpu_q1":q1, "cpu_q2":q2, "cpu_q3":q3, "cpu_p95":p95, "cpu_me":marginError, "cpu_ci095_min":CILow, "cpu_ci095_max":CIHigh}]

def f1(r):
    if r['cpu_percent_usage'] == None:
        return (0, 1)
    else:
        return (r['cpu_percent_usage'], 1)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("cpu_percent_usage") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .map(f1) \
        .cache()

data = dataRDD.sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()

query = computeMetrics(data)
sc.parallelize(query).saveToCassandra(cassandraKeyspace, "trial_cpu")


def f2(r):
    if r['cpu_percpu_percent_usage'] == None:
        return (None, 1)
    else:
        return (r['cpu_percpu_percent_usage'], 1)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("cpu_percpu_percent_usage") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .map(f2) \
        .cache()

nOfCores = len((dataRDD.first())[0])
for i in range(nOfCores):
    data = dataRDD.filter(lambda r: r[0] is not None) \
            .map(lambda r: (r[0][i], 1)) \
            .sortByKey(0, 1) \
            .map(lambda x: x[0]) \
            .collect()

    query = computeMetrics(data)
    query[0]["core"] = i
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "trial_cpu_core")