import sys
import json
import io
import gzip
import uuid

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def maxIOValues(dataRDD):
    if(dataRDD.isEmpty()):
        return []
    
    def whichHigher(a, b):
        if a is None or b is None:
            return None
        if a>b:
            return a
        else:
            return b
    
    data = dataRDD.collect()
    
    devices = {}
    for e in data:
        dev = e["device"]
        devices[dev] = 0
    
    queries = []
    
    for d in devices.keys():
        maxReads = dataRDD.filter(lambda a: a["device"] == d) \
            .map(lambda a: a["reads"]) \
            .reduce(whichHigher)
    
        maxWrites = dataRDD.filter(lambda a: a["device"] == d) \
            .map(lambda a: a["writes"]) \
            .reduce(whichHigher)
    
        maxTotal = dataRDD.filter(lambda a: a["device"] == d) \
            .map(lambda a: a["total"]) \
            .reduce(whichHigher)
    
        queries.append({"device":d, "reads":maxReads, "writes":maxWrites, "total":maxTotal})
    return queries

def createQueries(dataRDD, trialID, experimentID, containerID):
    queries = []
    result = maxIOValues(dataRDD)
    for e in result:
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "device":e["device"], "reads":e["reads"], "writes":e["writes"], "total":e["total"]})
    return queries

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments: Spark master, Cassandra host, Minio host, path of the files
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    containerID = sys.argv[4]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("IO analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "io_data"
    destTable = "trial_io"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable)\
            .select("device", "reads", "writes", "total") \
            .where("trial_id=? AND experiment_id=? AND container_id=?", trialID, experimentID, containerID) \
            .cache()
    
    # Generate queries for devices
    queries = createQueries(dataRDD, trialID, experimentID, containerID)
    
    # Save to Cassandra
    sc.parallelize(queries).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()