import sys
import json
#import urllib.request
import urllib2
import io
import gzip
import uuid

from datetime import timedelta

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
trialID = sys.argv[3]
minioHost = sys.argv[4]
filePath = sys.argv[5]
experimentID = trialID.split("_")[0]
minioPort = "9000"
cassandraKeyspace = "benchflow"
destTable = "trial_byte_size"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Stats Transformer") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath)
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
decompressed.seek(0,2)
dumpSize = decompressed.tell()

query = [{"experiment_id":experimentID, "trial_id":trialID, "size":dumpSize}]

sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
