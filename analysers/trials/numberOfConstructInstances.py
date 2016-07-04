import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID):
    queries = []
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("to_ignore", "source_construct_instance_id", "construct_name", "construct_type", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["source_construct_instance_id"] is not None and r["to_ignore"] is False) \
            .cache()
    
    numberOfInstances = dataRDD.count()
    
    queries.append({"experiment_id":experimentID, "trial_id":trialID, "number_of_construct_instances":numberOfInstances, "construct_type":"all", "construct_name": "all"})
    
    combinations = dataRDD.map(lambda a: (a["construct_type"], a["construct_name"])).distinct().collect()
    
    for combs in combinations:
        consType = combs[0]
        name = combs[1]
        
        numberOfInstances = dataRDD.filter(lambda a: a["construct_name"] == name and a["construct_type"] == consType).count()
        
        # Checking for type and name being None, in order to avoid saving a None type to the trials table
        if consType is None:
            consType = "Unspecified"
        if name is None:
            name = "Unspecified"
        
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "number_of_construct_instances":numberOfInstances, "construct_type":consType, "construct_name": name})

    return queries

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of construct instances analyser")
    sc = CassandraSparkContext(conf=conf)
    
    #analyserConf = getAnalyserConf(SUTName)
    srcTable = "construct"
    destTable = "trial_number_of_construct_instances"
        
    query = createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()