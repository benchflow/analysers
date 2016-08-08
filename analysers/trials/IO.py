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

def createQueries(dataRDD, trialID, experimentID, containerID, containerName, hostID):
    queries = []
    result = maxIOValues(dataRDD)
    for e in result:
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "container_name":containerName, "host_id":hostID, \
                        "device":e["device"], "reads":e["reads"], "writes":e["writes"], "total":e["total"]})
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    containerID = str(args["container_id"])
    containerName = str(args["container_name"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    partitionsPerCore = 5
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("IO analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "io_data"
    destTable = "trial_io"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
            .select("device", "reads", "writes", "total") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .repartition(sc.defaultParallelism * partitionsPerCore) \
            .cache()
    
    # Generate queries for devices
    queries = createQueries(dataRDD, trialID, experimentID, containerID, containerName, hostID)
    
    # Save to Cassandra
    sc.parallelize(queries, sc.defaultParallelism * partitionsPerCore).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__': main()