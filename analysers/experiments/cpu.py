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
        .where("experiment_id=?", experimentID) \
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
medianMin = sortAndGet("cpu_median", 1)
medianMax = sortAndGet("cpu_median", 0)

modeMin = CassandraRDD.select("cpu_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (min(x["cpu_mode"]), 0)) \
    .sortByKey(1, 1) \
    .map(lambda x: x[0]) \
    .first()
    
modeMax = CassandraRDD.select("cpu_mode") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: (max(x["cpu_mode"]), 0)) \
    .sortByKey(0, 1) \
    .map(lambda x: x[0]) \
    .first()
    
weightSum = CassandraRDD.select("cpu_weight") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["cpu_weight"]) \
    .reduce(lambda a, b: a+b)
    
weightedSum = CassandraRDD.select("cpu_weight", "cpu_mean") \
    .where("experiment_id=?", experimentID) \
    .map(lambda x: x["cpu_mean"]*x["cpu_weight"]) \
    .reduce(lambda a, b: a+b)
    
weightedMean = weightedSum/weightSum


# TODO: Fix this
query = [{"experiment_id":experimentID, "cpu_mode":[modeMin, modeMax], "cpu_median":[medianMin, medianMax], \
          "cpu_mean":[medianMin, medianMax], "cpu_min":dataMin, "cpu_max":dataMax, "cpu_q1":[q1Min, q1Max], "cpu_q2":[q2Min, q2Max], \
          "cpu_q3":[q3Min, q3Max]}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data[0])