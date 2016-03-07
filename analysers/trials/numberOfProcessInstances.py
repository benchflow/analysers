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
nToIgnore = 5
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "process"
destTable = "trial_number_of_process_instances"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Number of process instances analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("process_definition_id", "end_time", "start_time", "duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .filter(lambda r: r["process_definition_id"] is not None) \
        .cache()

processes = dataRDD.map(lambda r: r["process_definition_id"]) \
        .distinct() \
        .collect()

maxTime = None
for p in processes:
    time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
        .map(lambda r: (r["end_time"], 0)) \
        .sortByKey(1, 1) \
        .collect()
    time = time[nToIgnore-1]
    if maxTime is None or time[0] > maxTime:
        maxTime = time[0]

print(maxTime)

data = dataRDD.filter(lambda r: r["start_time"] > maxTime) \
        .map(lambda r: 1) \
        .reduce(lambda a, b: a+b)

query = [{"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":data}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)