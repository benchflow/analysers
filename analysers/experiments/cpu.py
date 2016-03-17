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
srcTable = "trial_cpu"
destTable = "exp_cpu"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Cpu analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

CassandraRDD = sc.cassandraTable(cassandraKeyspace, "trial_cpu")
CassandraRDD.cache()

nOfCores = CassandraRDD.select("cpu_cores") \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .first()

nOfCores = nOfCores["cpu_cores"]

def sortAndGet(field, asc):
    v = CassandraRDD.select(field) \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: (x[field], 0)) \
        .sortByKey(asc, 1) \
        .map(lambda x: x[0]) \
        .first()
    return v

dataMin = sortAndGet("cpu_min", 1)
dataMax = sortAndGet("cpu_max", 0)
q1Min = sortAndGet("cpu_q1", 1)
q1Max = sortAndGet("cpu_q1", 0)
q2Min = sortAndGet("cpu_q2", 1)
q2Max = sortAndGet("cpu_q2", 0)
q3Min = sortAndGet("cpu_q3", 1)
q3Max = sortAndGet("cpu_q3", 0)
p95Min = sortAndGet("cpu_p95", 1)
p95Max = sortAndGet("cpu_p95", 0)
medianMin = sortAndGet("cpu_median", 1)
medianMax = sortAndGet("cpu_median", 0)
    
weightSum = CassandraRDD.select("cpu_num_data_points") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: x["cpu_num_data_points"]) \
    .reduce(lambda a, b: a+b)
    
weightedSum = CassandraRDD.select("cpu_num_data_points", "cpu_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: x["cpu_mean"]*x["cpu_num_data_points"]) \
    .reduce(lambda a, b: a+b)

weightedMean = weightedSum/weightSum

meanMin = sortAndGet("cpu_mean", 1)
meMin = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMin) \
    .map(lambda x: (x["cpu_me"], 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
bestTrials = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMin and x["cpu_me"] == meMin) \
    .map(lambda x: x["trial_id"]) \
    .collect()

meanMax = sortAndGet("cpu_mean", 0)
meMax = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMax) \
    .map(lambda x: (x["cpu_me"], 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
worstTrials = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMax and x["cpu_me"] == meMax) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
meanAverage = CassandraRDD.select("trial_id", "cpu_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["cpu_mean"], 1)) \
    .reduce(lambda a, b: a+b)
meanAverage = meanAverage[0]/meanAverage[1]
meAverage = CassandraRDD.select("trial_id", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["cpu_me"], 1)) \
    .reduce(lambda a, b: a+b)
meAverage = meAverage[0]/meAverage[1]
averageTrialsUpperMean = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["cpu_mean"], x["trial_id"])) \
    .sortByKey(1, 1) \
    .filter(lambda x: x[0] >= meanAverage) \
    .map(lambda x: x[0]) \
    .first()
averageTrialsLowerMean = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["cpu_mean"], x["trial_id"])) \
    .sortByKey(0, 1) \
    .filter(lambda x: x[0] <= meanAverage) \
    .map(lambda x: x[0]) \
    .first()
averageTrials = CassandraRDD.select("trial_id", "cpu_mean", "cpu_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == averageTrialsUpperMean or x["cpu_mean"] == averageTrialsLowerMean) \
    .map(lambda x: x["trial_id"]) \
    .collect()

data = CassandraRDD.select("cpu_integral") \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: (x["cpu_integral"], 0)) \
        .sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()

integralDataMin = data[-1]
integralDataMax = data[0]
integralDataLength = len(data)
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

# TODO: Fix this
query = [{"experiment_id":experimentID, "container_id":containerID, "cpu_cores":nOfCores, \
          "cpu_median_min":medianMin, "cpu_median_max":medianMax, \
          "cpu_mean_min":medianMin, "cpu_mean_max":medianMax, \
          "cpu_min":dataMin, "cpu_max":dataMax, "cpu_q1_min":q1Min, \
          "cpu_q1_max":q1Max, "cpu_q2_min":q2Min, "cpu_q2_max":q2Max, \
          "cpu_p95_max":p95Max, "cpu_p95_min":p95Min, \
          "cpu_q3_min":q3Min, "cpu_q3_max":q3Max, "cpu_weighted_avg":weightedMean, \
          "cpu_best": bestTrials, "cpu_worst": worstTrials, "cpu_average": averageTrials, \
          "cpu_integral_median":median, "cpu_integral_mean":mean, "cpu_integral_avg":mean, \
          "cpu_integral_min":integralDataMin, "cpu_integral_max":integralDataMax, "cpu_integral_sd":stdD, \
          "cpu_integral_q1":q1, "cpu_integral_q2":q2, "cpu_integral_q3":q3, "cpu_integral_p95":p95, \
          "cpu_integral_me":marginError, "cpu_integral_ci095_min":CILow, "cpu_integral_ci095_max":CIHigh}]

print(query)

sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_cpu")


#####################################################


CassandraRDD = sc.cassandraTable(cassandraKeyspace, "trial_cpu_core")
CassandraRDD.cache()

def sortAndGetCore(field, asc, i):
    v = CassandraRDD.select(field) \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: (x[field][i], 0)) \
        .sortByKey(asc, 1) \
        .map(lambda x: x[0]) \
        .first()
    return v

# TODO: Fix this
query = [{"experiment_id":experimentID, "container_id":containerID, "cpu_cores":nOfCores, \
          "cpu_median_min":[None]*nOfCores, "cpu_median_max":[None]*nOfCores, \
          "cpu_mean_min":[None]*nOfCores, "cpu_mean_max":[None]*nOfCores, \
          "cpu_min":[None]*nOfCores, "cpu_max":[None]*nOfCores, "cpu_q1_min":[None]*nOfCores, \
          "cpu_q1_max":[None]*nOfCores, "cpu_q2_min":[None]*nOfCores, "cpu_q2_max":[None]*nOfCores, \
          "cpu_p95_max":[None]*nOfCores, "cpu_p95_min":[None]*nOfCores, \
          "cpu_q3_min":[None]*nOfCores, "cpu_q3_max":[None]*nOfCores, "cpu_weighted_avg":[None]*nOfCores}]

for i in range(nOfCores):
    dataMin = sortAndGetCore("cpu_min", 1, i)
    dataMax = sortAndGetCore("cpu_max", 0, i)
    q1Min = sortAndGetCore("cpu_q1", 1, i)
    q1Max = sortAndGetCore("cpu_q1", 0, i)
    q2Min = sortAndGetCore("cpu_q2", 1, i)
    q2Max = sortAndGetCore("cpu_q2", 0, i)
    q3Min = sortAndGetCore("cpu_q3", 1, i)
    q3Max = sortAndGetCore("cpu_q3", 0, i)
    p95Min = sortAndGetCore("cpu_p95", 1, i)
    p95Max = sortAndGetCore("cpu_p95", 0, i)
    medianMin = sortAndGetCore("cpu_median", 1, i)
    medianMax = sortAndGetCore("cpu_median", 0, i)
    meanMin = sortAndGetCore("cpu_mean", 1, i)
    meanMax = sortAndGetCore("cpu_mean", 0, i)
        
    weightSum = CassandraRDD.select("cpu_num_data_points") \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: x["cpu_num_data_points"]) \
        .reduce(lambda a, b: a+b)
        
    weightedSum = CassandraRDD.select("cpu_num_data_points", "cpu_mean") \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: x["cpu_mean"][i]*x["cpu_num_data_points"]) \
        .reduce(lambda a, b: a+b)

    weightedMean = weightedSum/weightSum
    query[0]["cpu_weighted_avg"][i] = weightedMean
    query[0]["cpu_median_min"][i] = medianMin
    query[0]["cpu_median_max"][i] = medianMax
    query[0]["cpu_mean_min"][i] = meanMin
    query[0]["cpu_mean_max"][i] = meanMax
    query[0]["cpu_min"][i] = dataMin
    query[0]["cpu_max"][i] = dataMax
    query[0]["cpu_q1_min"][i] = q1Min
    query[0]["cpu_q1_max"][i] = q1Max
    query[0]["cpu_q2_min"][i] = q2Min
    query[0]["cpu_q2_max"][i] = q2Max
    query[0]["cpu_q3_min"][i] = q3Min
    query[0]["cpu_q3_max"][i] = q3Max
    query[0]["cpu_p95_min"][i] = p95Min
    query[0]["cpu_p95_max"][i] = p95Max

sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_cpu_core")