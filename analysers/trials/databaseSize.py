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

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Database size analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "database_sizes"
    destTable = "trial_byte_size"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("size") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
    
    query = createQuery(dataRDD, experimentID, trialID)
    
    # Saves to Cassandra
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()