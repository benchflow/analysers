import sys
import json
import io
import gzip
import uuid

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
experimentID = trialID.split("_")[0]
cassandraKeyspace = "benchflow"
srcTable = "database_sizes"
destTable = "trial_byte_size"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Database size analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

data = sc.cassandraTable(cassandraKeyspace, srcTable)\
        .select("size") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .reduce(lambda a, b: a["size"]+b["size"])

query = [{"experiment_id":experimentID, "trial_id":trialID, "size":data["size"]}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))

print(data)