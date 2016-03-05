import sys
import json
import io
import gzip
import uuid

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
containerID = sys.argv[4]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "io_data"
destTable = "trial_IO"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("IO analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

def f(a, b):
    if a>b:
        return a
    else:
        return b

data = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("device") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .collect()

devices = {}
for e in data:
    dev = e["device"]
    devices[dev] = 0

queries = []

for d in devices.keys():
    maxReads = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("device", "reads") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .filter(lambda a: a["device"] == d) \
        .map(lambda a: a["reads"]) \
        .reduce(f)

    maxWrites = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("device", "writes") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .filter(lambda a: a["device"] == d) \
        .map(lambda a: a["writes"]) \
        .reduce(f)

    maxTotal = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("device", "total") \
        .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
        .filter(lambda a: a["device"] == d) \
        .map(lambda a: a["total"]) \
        .reduce(f)

    queries.append({"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "device":d, "reads":maxReads, "writes":maxWrites, "total":maxTotal})


sc.parallelize(queries).saveToCassandra(cassandraKeyspace, destTable)
