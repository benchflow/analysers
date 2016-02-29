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
        .where("experiment_id=?", experimentID) \
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

modeMin = CassandraRDD.select("ram_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (min(x["ram_mode"]), 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
    
modeMax = CassandraRDD.select("ram_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (max(x["ram_mode"]), 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
    
weightSum = CassandraRDD.select("ram_num_data_points") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["ram_num_data_points"]) \
    .reduce(lambda a, b: a+b)
    
weightedSum = CassandraRDD.select("ram_num_data_points", "ram_mean") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["ram_mean"]*x["ram_num_data_points"]) \
    .reduce(lambda a, b: a+b)

weightedMean = weightedSum/weightSum

meanMin = sortAndGet("ram_mean", 1)
bestTrials = CassandraRDD.select("trial_id", "ram_mean") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["ram_mean"] == meanMin) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
meanMax = sortAndGet("ram_mean", 0)
worstTrials = CassandraRDD.select("trial_id", "ram_mean") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["ram_mean"] == meanMax) \
    .map(lambda x: x["trial_id"]) \
    .collect()
    
averageTrials = CassandraRDD.select("trial_id") \
    .where("experiment_id=?", experimentID) \
    .filter(lambda x: x["trial_id"] not in bestTrials and x["trial_id"] not in worstTrials) \
    .map(lambda x: x["trial_id"]) \
    .collect()

# TODO: Fix this
query = [{"experiment_id":experimentID, "ram_mode_min":modeMin, "ram_mode_max":modeMax, \
          "ram_median_min":medianMin, "ram_median_max":medianMax, \
          "ram_mean_min":medianMin, "ram_mean_max":medianMax, \
          "ram_min":dataMin, "ram_max":dataMax, "ram_q1_min":q1Min, \
          "ram_q1_max":q1Max, "ram_q2_min":q2Min, "ram_q2_max":q2Max, \
          "ram_p95_max":p95Max, "ram_p95_min":p95Min, \
          "ram_q3_min":q3Min, "ram_q3_max":q3Max, "ram_weighted_avg":weightedMean, \
          "ram_best": bestTrials, "ram_worst": worstTrials, "ram_average": averageTrials}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)