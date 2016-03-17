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
srcTable = "trial_ram"
destTable = "exp_ram"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Ram analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable)
CassandraRDD.cache()

def sortAndGet(field, asc):
    v = CassandraRDD.select(field) \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: (x[field], 0)) \
        .sortByKey(asc, 1) \
        .map(lambda x: x[0]) \
        .first()
    return v

dataMin = sortAndGet("ram_min", 1)
dataMax = sortAndGet("ram_max", 0)
q1Min = sortAndGet("ram_q1", 1)
q1Max = sortAndGet("ram_q1", 0)
q2Min = sortAndGet("ram_q2", 1)
q2Max = sortAndGet("ram_q2", 0)
q3Min = sortAndGet("ram_q3", 1)
q3Max = sortAndGet("ram_q3", 0)
p95Min = sortAndGet("ram_p95", 1)
p95Max = sortAndGet("ram_p95", 0)
medianMin = sortAndGet("ram_median", 1)
medianMax = sortAndGet("ram_median", 0)

modeMinValues = CassandraRDD.select("ram_mode", "ram_mode_freq") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (min(x["ram_mode"]), x["ram_mode_freq"])) \
    .sortByKey(1, 1) \
    .map(lambda x: (x[0], x[1])) \
    .first()
modeMin = modeMinValues[0]
modeMinFreq = modeMinValues[1]

modeMaxValues = CassandraRDD.select("ram_mode", "ram_mode_freq") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (max(x["ram_mode"]), x["ram_mode_freq"])) \
    .sortByKey(0, 1) \
    .map(lambda x: (x[0], x[1])) \
    .first()
modeMax = modeMaxValues[0]
modeMaxFreq = modeMaxValues[1]
    
weightSum = CassandraRDD.select("ram_num_data_points") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: x["ram_num_data_points"]) \
    .reduce(lambda a, b: a+b)
    
weightedSum = CassandraRDD.select("ram_num_data_points", "ram_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: x["ram_mean"]*x["ram_num_data_points"]) \
    .reduce(lambda a, b: a+b)

weightedMean = weightedSum/weightSum

meanMin = sortAndGet("ram_mean", 1)
meMin = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["ram_mean"] == meanMin) \
    .map(lambda x: (x["ram_me"], 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
bestTrials = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["ram_mean"] == meanMin and x["ram_me"] == meMin) \
    .map(lambda x: x["trial_id"]) \
    .collect()

meanMax = sortAndGet("ram_mean", 0)
meMax = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["ram_mean"] == meanMax) \
    .map(lambda x: (x["ram_me"], 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
worstTrials = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["ram_mean"] == meanMax and x["ram_me"] == meMax) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
meanAverage = CassandraRDD.select("trial_id", "ram_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["ram_mean"], 1)) \
    .reduce(lambda a, b: a+b)
meanAverage = meanAverage[0]/meanAverage[1]
meAverage = CassandraRDD.select("trial_id", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["ram_me"], 1)) \
    .reduce(lambda a, b: a+b)
meAverage = meAverage[0]/meAverage[1]
averageTrialsUpperMean = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["ram_mean"], x["trial_id"])) \
    .sortByKey(1, 1) \
    .filter(lambda x: x[0] >= meanAverage) \
    .map(lambda x: x[0]) \
    .first()
averageTrialsLowerMean = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (x["ram_mean"], x["trial_id"])) \
    .sortByKey(0, 1) \
    .filter(lambda x: x[0] <= meanAverage) \
    .map(lambda x: x[0]) \
    .first()
averageTrials = CassandraRDD.select("trial_id", "ram_mean", "ram_me") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["ram_mean"] == averageTrialsUpperMean or x["ram_mean"] == averageTrialsLowerMean) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
data = CassandraRDD.select("ram_integral") \
        .where("experiment_id=? AND container_id=?", experimentID, containerID) \
        .map(lambda x: (x["ram_integral"], 0)) \
        .sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()

integralDataMin = data[-1]
integralDataMax = data[0]
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

# TODO: Fix this
query = [{"experiment_id":experimentID, "container_id":containerID, "ram_mode_min":modeMin, "ram_mode_max":modeMax, \
          "ram_mode_min_freq":modeMinFreq, "ram_mode_max_freq":modeMaxFreq, \
          "ram_median_min":medianMin, "ram_median_max":medianMax, \
          "ram_mean_min":medianMin, "ram_mean_max":medianMax, \
          "ram_min":dataMin, "ram_max":dataMax, "ram_q1_min":q1Min, \
          "ram_q1_max":q1Max, "ram_q2_min":q2Min, "ram_q2_max":q2Max, \
          "ram_p95_max":p95Max, "ram_p95_min":p95Min, \
          "ram_q3_min":q3Min, "ram_q3_max":q3Max, "ram_weighted_avg":weightedMean, \
          "ram_best": bestTrials, "ram_worst": worstTrials, "ram_average": averageTrials, \
          "ram_integral_median":median, "ram_integral_mean":mean, "ram_integral_avg":mean, \
          "ram_integral_min":integralDataMin, "ram_integral_max":integralDataMax, "ram_integral_sd":stdD, \
          "ram_integral_q1":q1, "ram_integral_q2":q2, "ram_integral_q3":q3, "ram_integral_p95":p95, \
          "ram_integral_me":marginError, "ram_integral_ci095_min":CILow, "ram_integral_ci095_max":CIHigh}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)