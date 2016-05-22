import sys
import json
import io
import gzip
import uuid

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def databaseSize(dataRDD):
    if dataRDD.isEmpty():
        return None
    data = dataRDD.reduce(lambda a, b: a["size"]+b["size"])
    if isinstance(data, int):
        return data
    else:
        return data["size"]
    
def createQuery(dataRDD, experimentID, trialID):
    size = databaseSize(dataRDD)
    return [{"experiment_id":experimentID, "trial_id":trialID, "size":size}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Database size analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "database_sizes"
    destTable = "trial_byte_size"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("size") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
    
    query = createQuery(dataRDD, experimentID, trialID)
    
    # Saves to Cassandra
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()