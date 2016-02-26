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
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "trial_process_duration"
destTable = "exp_process_duration"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Process duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable)
CassandraRDD.cache()

def sortAndGet(field, asc):
    v = CassandraRDD.select(field) \
        .where("experiment_id=?", experimentID) \
        .map(lambda x: (x[field], 0)) \
        .sortByKey(asc, 1) \
        .map(lambda x: x[0]) \
        .first()
    return v

dataMin = sortAndGet("process_duration_min", 1)
dataMax = sortAndGet("process_duration_max", 0)
q1Min = sortAndGet("process_duration_q1", 1)
q1Max = sortAndGet("process_duration_q1", 0)
q2Min = sortAndGet("process_duration_q2", 1)
q2Max = sortAndGet("process_duration_q2", 0)
q3Min = sortAndGet("process_duration_q3", 1)
q3Max = sortAndGet("process_duration_q3", 0)
p95Min = sortAndGet("process_duration_p95", 1)
p95Max = sortAndGet("process_duration_p95", 0)
medianMin = sortAndGet("process_duration_median", 1)
medianMax = sortAndGet("process_duration_median", 0)

modeMin = CassandraRDD.select("process_duration_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (min(x["process_duration_mode"]), 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
    
modeMax = CassandraRDD.select("process_duration_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (max(x["process_duration_mode"]), 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
    
weightSum = CassandraRDD.select("process_duration_num_data_points") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["process_duration_num_data_points"]) \
    .reduce(lambda a, b: a+b)
    
weightedSum = CassandraRDD.select("process_duration_num_data_points", "process_duration_mean") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["process_duration_mean"]*x["process_duration_num_data_points"]) \
    .reduce(lambda a, b: a+b)

weightedMean = weightedSum/weightSum

meanMin = sortAndGet("process_duration_mean", 1)
bestTrials = CassandraRDD.select("trial_id", "process_duration_mean") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["process_duration_mean"] == meanMin) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
meanMax = sortAndGet("process_duration_mean", 0)
worstTrials = CassandraRDD.select("trial_id", "process_duration_mean") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["process_duration_mean"] == meanMax) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
averageTrials = CassandraRDD.select("trial_id") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["trial_id"] not in bestTrials and x["trial_id"] not in worstTrials) \
    .collect()

# TODO: Fix this
query = [{"experiment_id":experimentID, "process_duration_mode_min":modeMin, "process_duration_mode_max":modeMax, \
          "process_duration_median_min":medianMin, "process_duration_median_max":medianMax, \
          "process_duration_mean_min":medianMin, "process_duration_mean_max":medianMax, \
          "process_duration_min":dataMin, "process_duration_max":dataMax, "process_duration_q1_min":q1Min, \
          "process_duration_q1_max":q1Max, "process_duration_q2_min":q2Min, "process_duration_q2_max":q2Max, \
          "process_duration_p95_max":p95Max, "process_duration_p95_min":p95Min, \
          "process_duration_q3_min":q3Min, "process_duration_q3_max":q3Max, "process_duration_weighted_avg":weightedMean, \
          "process_duration_best": bestTrials, "process_duration_worst": worstTrials, "process_duration_average": averageTrials}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)