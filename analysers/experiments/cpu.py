import sys
import json
import io
import gzip
import uuid
import math

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

modeMin = CassandraRDD.select("cpu_mode") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (min(x["cpu_mode"]), 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
    
modeMax = CassandraRDD.select("cpu_mode") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .map(lambda x: (max(x["cpu_mode"]), 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
    
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
bestTrials = CassandraRDD.select("trial_id", "cpu_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMin) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
meanMax = sortAndGet("cpu_mean", 0)
worstTrials = CassandraRDD.select("trial_id", "cpu_mean") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["cpu_mean"] == meanMax) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
averageTrials = CassandraRDD.select("trial_id") \
    .where("experiment_id=? AND container_id=?", experimentID, containerID) \
    .filter(lambda x: x["trial_id"] not in bestTrials and x["trial_id"] not in worstTrials) \
    .map(lambda x: x["trial_id"]) \
    .collect()

# TODO: Fix this
query = [{"experiment_id":experimentID, "container_id":containerID, "cpu_mode_min":modeMin, "cpu_mode_max":modeMax, \
          "cpu_median_min":medianMin, "cpu_median_max":medianMax, \
          "cpu_mean_min":medianMin, "cpu_mean_max":medianMax, \
          "cpu_min":dataMin, "cpu_max":dataMax, "cpu_q1_min":q1Min, \
          "cpu_q1_max":q1Max, "cpu_q2_min":q2Min, "cpu_q2_max":q2Max, \
          "cpu_p95_max":p95Max, "cpu_p95_min":p95Min, \
          "cpu_q3_min":q3Min, "cpu_q3_max":q3Max, "cpu_weighted_avg":weightedMean, \
          "cpu_best": bestTrials, "cpu_worst": worstTrials, "cpu_average": averageTrials}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)