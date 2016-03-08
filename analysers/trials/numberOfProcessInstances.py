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
        .select("process_definition_id", "source_process_instance_id", "start_time", "duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .filter(lambda r: r["process_definition_id"] is not None) \
        .cache()

processes = dataRDD.map(lambda r: r["process_definition_id"]) \
        .distinct() \
        .collect()

maxTime = None
maxID = None
for p in processes:
    time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
        .map(lambda r: (r["start_time"], r["source_process_instance_id"])) \
        .sortByKey(1, 1) \
        .collect()
    if len(time) < nToIgnore:
        continue
    else:
        time = time[nToIgnore-1]
    if maxTime is None or time[0] > maxTime:
        maxTime = time[0]
        defId = dataRDD.filter(lambda r: r["process_definition_id"] == p and r["start_time"] == maxTime) \
            .map(lambda r: (r["source_process_instance_id"], 0)) \
            .sortByKey(1, 1) \
            .first()
        maxID = defId[0]

print(maxTime)

data = dataRDD.map(lambda r: (r["start_time"], r)) \
        .sortByKey(1, 1) \
        .map(lambda r: r[1]) \
        .collect()

index = -1
if maxID is not None:
    for i in range(len(data)):
        if data[i]["source_process_instance_id"] == maxID:
            index = i
            break

data = data[index+1:]
data = len(data)

query = [{"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":data}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Number of process instances analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("process_definition_id", "source_process_instance_id", "end_time", "start_time", "duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .filter(lambda r: r["process_definition_id"] is not None) \
        .cache()

processes = dataRDD.map(lambda r: r["process_definition_id"]) \
        .distinct() \
        .collect()

maxTime = None
maxID = None
for p in processes:
    time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
        .map(lambda r: (r["start_time"], r["source_process_instance_id"])) \
        .sortByKey(1, 1) \
        .collect()
    time = time[nToIgnore-1]
    if maxTime is None or time[0] > maxTime:
        maxTime = time[0]
        maxID = time[1]

print(maxTime)

data = dataRDD.map(lambda r: (r["start_time"], r)) \
        .sortByKey(1, 1) \
        .map(lambda r: r[1]) \
        .collect()

index = 0
for i in range(len(data)):
    if data[i]["source_process_instance_id"] == maxID:
        index = i
        break

data = sc.parallelize(data[index:])

data = data.map(lambda r: 1) \
        .reduce(lambda a, b: a+b)

query = [{"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":data}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)