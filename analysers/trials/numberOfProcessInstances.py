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

data = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("duration") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(lambda r: 1) \
        .reduce(lambda a, b: a+b)

query = [{"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":data}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)