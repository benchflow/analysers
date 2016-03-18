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
    dataIntegral = integrate.trapz(data).item()

    # TODO: Fix this
    return [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "cpu_median":median, "cpu_cores":nOfCores, \
              "cpu_mean":mean, "cpu_avg":mean, "cpu_integral":dataIntegral, "cpu_num_data_points":dataLength, \
              "cpu_min":dataMin, "cpu_max":dataMax, "cpu_sd":stdD, \
              "cpu_q1":q1, "cpu_q2":q2, "cpu_q3":q3, "cpu_p95":p95, "cpu_me":marginError, "cpu_ci095_min":CILow, "cpu_ci095_max":CIHigh}]

def f1(r):
    if r['cpu_percent_usage'] == None:
        return (0, 1)
    else:
        return (r['cpu_percent_usage'], 1)

nOfCores = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("cpu_cores") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .first()
        
nOfCores = nOfCores["cpu_cores"]

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
        .filter(lambda r: r[0] is not None) \
        .cache()

query = [{}]
query[0]["experiment_id"] = experimentID
query[0]["trial_id"] = trialID
query[0]["container_id"] = containerID
query[0]["cpu_cores"] = nOfCores
query[0]["cpu_num_data_points"] = None
query[0]["cpu_median"] = [None]*nOfCores
query[0]["cpu_mean"] = [None]*nOfCores
query[0]["cpu_avg"] = [None]*nOfCores
query[0]["cpu_integral"] = [None]*nOfCores
query[0]["cpu_min"] = [None]*nOfCores
query[0]["cpu_max"] = [None]*nOfCores
query[0]["cpu_sd"] = [None]*nOfCores
query[0]["cpu_q1"] = [None]*nOfCores
query[0]["cpu_q2"] = [None]*nOfCores
query[0]["cpu_q3"] = [None]*nOfCores
query[0]["cpu_p95"] = [None]*nOfCores
query[0]["cpu_me"] = [None]*nOfCores
query[0]["cpu_ci095_min"] = [None]*nOfCores
query[0]["cpu_ci095_max"] = [None]*nOfCores

for i in range(nOfCores):
    data = dataRDD.map(lambda r: (r[0][i], 1)) \
            .sortByKey(0, 1) \
            .map(lambda x: x[0]) \
            .collect()

    met = computeMetrics(data)
    query[0]["cpu_num_data_points"] = met[0]["cpu_num_data_points"]
    query[0]["cpu_median"][i] = met[0]["cpu_median"]
    query[0]["cpu_mean"][i] = met[0]["cpu_mean"]
    query[0]["cpu_avg"][i] = met[0]["cpu_avg"]
    query[0]["cpu_integral"][i] = met[0]["cpu_integral"]
    query[0]["cpu_min"][i] = met[0]["cpu_min"]
    query[0]["cpu_max"][i] = met[0]["cpu_max"]
    query[0]["cpu_sd"][i] = met[0]["cpu_sd"]
    query[0]["cpu_q1"][i] = met[0]["cpu_q1"]
    query[0]["cpu_q2"][i] = met[0]["cpu_q2"]
    query[0]["cpu_q3"][i] = met[0]["cpu_q3"]
    query[0]["cpu_p95"][i] = met[0]["cpu_p95"]
    query[0]["cpu_me"][i] = met[0]["cpu_me"]
    query[0]["cpu_ci095_min"][i] = met[0]["cpu_ci095_min"]
    query[0]["cpu_ci095_max"][i] = met[0]["cpu_ci095_max"]

   
sc.parallelize(query).saveToCassandra(cassandraKeyspace, "trial_cpu_core", ttl=timedelta(hours=1))