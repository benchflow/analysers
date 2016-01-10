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
srcTable = "process"
destTable = "exp_throughput"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Process duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

def f(r):
    if r['duration'] == None:
        return (0, 0)
    else:
        return (r['duration'], 0)

data = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(f) \
        .sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .collect()
 
totalTime = 0
for d in data:
    totalTime += d
    
tp = len(data)/float(totalTime)

# TODO: Fix this
query = [{"experiment_id":trialID, "throughput":tp}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data[0])