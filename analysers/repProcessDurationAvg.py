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
destTable = "metrics"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Avg duration analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

data = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("duration") \
        .where("trial_id=?", trialID) \
        .where("experiment_id=?", experimentID) \
        .map(lambda r: (long(r["duration"]), 1)) \
        .reduce(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
        
avg = data[0]/data[1]
query = [{"trial_id":trialID, "duration_avg":avg}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)