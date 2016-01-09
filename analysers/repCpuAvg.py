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
srcTable = "environment_data"
destTable = "experiment_single_metrics"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Avg cpu analyser") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

def f(r):
    if r['cpu_percent_usage'] == None:
        return (0, 0)
    else:
        return (long(r["cpu_percent_usage"]), 1)

data = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("cpu_percent_usage") \
        .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
        .map(f) \
        .reduce(lambda a, b: (a[0]+b[0], a[1]+b[1]))
        
avg = data[0]/float(data[1])
# TODO: Fix this
query = [{"experiment_id":trialID, "cpu_avg":avg}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)

print(data)