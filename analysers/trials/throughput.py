import sys
import json
import io
import gzip
import uuid
import math
import datetime 

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
destTable = "trial_throughput"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Process duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# TODO: Use Spark for all computations

data = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("start_time", "end_time") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(lambda r: (r['start_time'], r['end_time'])) \
        .collect()

smallest = None
for d in data:
    t = d[0]
    if smallest == None:
        smallest = t
    elif t != None:
        if t < smallest:
            smallest = t
print(smallest)
        
largest = None
for d in data:
    t = d[1]
    if largest == None:
        largest = t
    elif t != None:
        if t > largest:
            largest = t
print(largest)
        
delta = largest - smallest
delta = delta.total_seconds()
print(delta)

tp = len(data)/float(delta)
print(tp)

# TODO: Fix this
query = [{"experiment_id":experimentID, "trial_id":trialID, "throughput":tp, "execution_time":delta}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)